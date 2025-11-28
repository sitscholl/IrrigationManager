from __future__ import annotations

import logging
from collections.abc import Sequence
from datetime import datetime
from typing import Optional
from dataclasses import dataclass

import pandas as pd
import pandera.pandas as pa
import requests
from pandera.errors import SchemaError

from .resample import MeteoResampler

logger = logging.getLogger(__name__)

@dataclass
class Station:
    id: str
    elevation: float
    latitude: float
    longitude: float
    data: pd.DataFrame

    def __post_init__(self):
        if -90 > self.latitude or self.latitude > 90:
            raise ValueError("Latitude must be between -90 and 90")
        if -180 > self.longitude or self.longitude > 180:
            raise ValueError("Longitude must be between -180 and 180")

        if self.elevation is None:
            try:
                self.elevation = self.get_elevation()
            except Exception as e:
                logger.warning(f"Fetching elevation for station {self.id} failed with error: {e}")

    def get_elevation(self):
        api_template = "https://api.opentopodata.org/v1/eudem25m?locations={lat},{lon}"
        url = api_template.format(lat=self.latitude, lon=self.longitude)
        response = requests.get(url)
        response.raise_for_status()
        elevation = response.json()["results"][0]["elevation"]
        return elevation

class MeteoHandler:
    """Manager class to query meteo data from multiple fields/stations and transform returned data to a consistent schema"""

    def __init__(self, config: dict, et0_calculator: Optional[object] = None):
        
        self.radiation_fallback_provider = config.get("radiation_fallback_provider", "province")
        self.radiation_fallback_station = config.get("radiation_fallback_station", "09700MS")
        self.request_timeout = config.get("request_timeout", 600)

        api_config = config["api"]
        self.api_host = api_config["host"].rstrip("/")
        self.query_template = api_config["query_template"].lstrip("/")

        self.et0_calculator = et0_calculator
        self._session = requests.Session()

        self.stations = []

    @property
    def output_schema(self) -> pa.DataFrameSchema:
        """
        Define the expected schema for meteorological data output.

        Returns:
            pa.DataFrameSchema: Schema for validating SBR output data
        """
        return pa.DataFrameSchema(
            {
                "station_id": pa.Column(str),
                "tair_2m": pa.Column(float, nullable=True, required=False),
                "relative_humidity": pa.Column(float, nullable=True, required=False),
                "wind_speed": pa.Column(float, nullable=True, required=False),
                "precipitation": pa.Column(float, nullable=True, required=False),
                "air_pressure": pa.Column(float, nullable=True, required=False),
                "sun_duration": pa.Column(float, nullable=True, required=False),
                "solar_radiation": pa.Column(float, nullable=True, required=False),
            },
            index=pa.Index(pd.DatetimeTZDtype(tz="UTC")),
            strict=False,  # Allow additional columns that might be added
        )

    def _validate(self, transformed_data: pd.DataFrame) -> pd.DataFrame:
        return self.output_schema.validate(transformed_data)

    def _build_url(
        self,
        provider: str,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> str:
        path = self.query_template.format(
            provider=provider,
            station_id=station_id,
            start_date=start,
            end_date=end,
        ).lstrip("/")
        return f"{self.api_host}/{path}"

    def _get_data(
        self,
        provider: str,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> pd.DataFrame:
        url = self._build_url(provider, station_id, start, end)
        try:
            response = self._session.get(url, timeout=self.request_timeout)
            response.raise_for_status()
            payload = response.json()

            raw_data = payload.get("data", [])
            if not raw_data:
                logger.warning("No data returned for station %s (%s)", station_id, provider)
                return pd.DataFrame(), None

            response_data = pd.DataFrame(raw_data)
            response_metadata = payload.get("metadata", {})

            if "datetime" not in response_data.columns:
                logger.error("Missing 'datetime' column in response from %s", url)
                return pd.DataFrame(), None

            response_data["datetime"] = pd.to_datetime(response_data["datetime"], utc=True)

        except (requests.exceptions.RequestException, ValueError) as exc:
            logger.error("Error fetching data from %s: %s", url, exc)
            return pd.DataFrame(), None

        response_data = response_data.set_index("datetime").sort_index()
        response_data["station_id"] = station_id
        response_data = self._convert_solar_radiation_units(response_data)
        return response_data, response_metadata

    def _convert_solar_radiation_units(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Convert solar radiation readings from W/m^2 to MJ/m^2 based on the sampling interval.
        """
        if "solar_radiation" not in df.columns or df["solar_radiation"].isna().all():
            return df

        if not isinstance(df.index, pd.DatetimeIndex):
            logger.warning("Cannot convert solar radiation units because dataframe index is not datetime.")
            return df

        intervals = df.index.to_series().diff().dt.total_seconds()
        positive_intervals = intervals[intervals > 0]

        if positive_intervals.empty:
            logger.warning("Unable to infer sampling interval for converting solar radiation units.")
            return df

        representative_interval = positive_intervals.median()
        intervals = intervals.fillna(representative_interval).where(intervals > 0, representative_interval)

        if intervals.isna().any() or (representative_interval is None) or representative_interval <= 0:
            logger.warning("Invalid sampling interval encountered while converting solar radiation units.")
            return df

        mj_factor = intervals / 1_000_000  # W -> J then to MJ
        converted_df = df.copy()
        converted_df["solar_radiation"] = converted_df["solar_radiation"] * mj_factor
        return converted_df

    def _fill_solar_radiation(
        self,
        df: pd.DataFrame,
        start: datetime,
        end: datetime,
    ) -> pd.DataFrame:
        if "solar_radiation" in df.columns and df["solar_radiation"].notna().any():
            return df

        if not self.radiation_fallback_station:
            return df

        fallback_df, _ = self._get_data(
            self.radiation_fallback_provider,
            self.radiation_fallback_station,
            start,
            end,
        )

        if fallback_df.empty or "solar_radiation" not in fallback_df.columns:
            logger.warning(
                "Unable to fetch fallback solar radiation data for station %s",
                self.radiation_fallback_station,
            )
            return df

        fallback_series = (
            fallback_df["solar_radiation"]
            .reindex(df.index, method='nearest', tolerance=pd.Timedelta('3min'))
            .interpolate(method="time", limit_direction="both")
            .bfill()
        )

        if fallback_series.isna().all():
            logger.warning("Fallback solar radiation series is empty for station %s", self.radiation_fallback_station)
            return df

        df = df.copy()
        df["solar_radiation"] = fallback_series
        return df

    def query(self, provider: str, station_ids: Sequence[str], start: datetime | str, end: datetime | str, resampler: MeteoResampler | None = None) -> pd.DataFrame:
        
        if isinstance(start, str):
            start = pd.to_datetime(start, dayfirst=True)
        if isinstance(end, str):
            end = pd.to_datetime(end, dayfirst=True)

        if start >= end:
            raise ValueError("start must be before end")

        stations: list[Station] = []
        for station in station_ids:
            try:
                df, metadata = self._get_data(provider, station, start, end)

                if df.empty:
                    logger.warning("No data returned for station %s", station)
                    continue

                df = self._fill_solar_radiation(df, start, end)

                validated_df = self._validate(df)
                stations.append(Station(station, metadata["elevation"], metadata["latitude"], metadata["longitude"], validated_df))
                logger.debug("Fetched data for station %s", station)
            except SchemaError as exc:
                logger.error("Schema validation failed for station %s: %s", station, exc)
            except Exception as exc:  # pragma: no cover - defensive logging
                logger.error("Unexpected error while fetching data for station %s: %s", station, exc)

        if resampler is not None:
            for i in stations:
                i.data = resampler.resample(i.data)

        self.stations = stations
        logger.info(f"Fetched data for {len(stations)} stations")

    def get_station_data(self, station_id: str) -> pd.DataFrame:
        for station in self.stations:
            if station.id == station_id:
                return station.data
        logger.error("Station %s not found", station_id)
        return pd.DataFrame()

    def calculate_et(self, et_calculator, correct: bool = True):
        """
        Adds et0 or et values to the station data, depending on correct.
        """
        for station in self.stations:
            station.data['et'] = et_calculator.calculate(station, correct)


if __name__ == '__main__':
    import matplotlib.pyplot as plt
    from .config import load_config
    from .resample import MeteoResampler

    config = load_config('config/config.yaml')

    handler = MeteoHandler(config)
    handler.query('SBR', ['103'], datetime(2025, 10, 1), datetime(2025, 10, 2), resampler = MeteoResampler(freq='D', min_count = 20))

    handler.stations[0].data['solar_radiation'].plot()
    print(handler.stations[0].dataata)
