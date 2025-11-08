from __future__ import annotations

import logging
from collections.abc import Sequence
from datetime import datetime
from typing import Optional

import pandas as pd
import pandera.pandas as pa
import requests
from pandera.errors import SchemaError

from .resample import MeteoResampler

logger = logging.getLogger(__name__)

class MeteoHandler:
    """Manager class to query meteo data from multiple fields/stations and transform returned data to a consistent schema"""

    def __init__(self, config: dict, et0_calculator: Optional[object] = None):
        api_config = config["api"]
        self.api_host = api_config["host"].rstrip("/")
        self.query_template = api_config["query_template"].lstrip("/")

        meteo_config = config.get("meteo", {})
        self.radiation_fallback_provider = meteo_config.get("radiation_fallback_provider", "province")
        self.radiation_fallback_station = meteo_config.get("radiation_fallback_station", "09700MS")
        self.request_timeout = meteo_config.get("request_timeout", 10)

        self.et0_calculator = et0_calculator
        self._session = requests.Session()

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
                return pd.DataFrame()

            response_data = pd.DataFrame(raw_data)

            if "datetime" not in response_data.columns:
                logger.error("Missing 'datetime' column in response from %s", url)
                return pd.DataFrame()

            response_data["datetime"] = pd.to_datetime(response_data["datetime"], utc=True)

        except (requests.exceptions.RequestException, ValueError) as exc:
            logger.error("Error fetching data from %s: %s", url, exc)
            return pd.DataFrame()

        response_data = response_data.set_index("datetime").sort_index()
        response_data["station_id"] = station_id
        return self._convert_solar_radiation_units(response_data)

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

        fallback_df = self._get_data(
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
            .reindex(df.index)
            .interpolate(method="time", limit_direction="both")
            .bfill()
        )

        if fallback_series.isna().all():
            logger.warning("Fallback solar radiation series is empty for station %s", self.radiation_fallback_station)
            return df

        df = df.copy()
        df["solar_radiation"] = fallback_series
        return df

    def _empty_dataframe(self) -> pd.DataFrame:
        column_names = list(self.output_schema.columns.keys())
        empty_df = pd.DataFrame(columns=column_names)
        empty_df.index = pd.DatetimeIndex([], tz="UTC")
        return empty_df

    def query(self, provider: str, station_ids: Sequence[str], start: datetime, end: datetime, resampler: MeteoResampler | None = None) -> pd.DataFrame:
        
        if start >= end:
            raise ValueError("start must be before end")

        data_frames: list[pd.DataFrame] = []
        for station in station_ids:
            try:
                df = self._get_data(provider, station, start, end)

                if df.empty:
                    logger.warning("No data returned for station %s", station)
                    continue

                df = self._fill_solar_radiation(df, start, end)

                validated_df = self._validate(df)
                data_frames.append(validated_df)
            except SchemaError as exc:
                logger.error("Schema validation failed for station %s: %s", station, exc)
            except Exception as exc:  # pragma: no cover - defensive logging
                logger.error("Unexpected error while fetching data for station %s: %s", station, exc)

        if not data_frames:
            return self._empty_dataframe()

        data = pd.concat(data_frames).sort_index()
        if resampler is not None:
            data = resampler.resample(data)

        return data

    def calculate_et0(self, meteo_data: pd.DataFrame, **kwargs):
        """
        Use the et0 calculator and its method to calculate evapotranspiration from the meteodata
        """
        pass

if __name__ == '__main__':
    import matplotlib.pyplot as plt
    from .config import load_config
    from .resample import MeteoResampler

    config = load_config('config/config.yaml')

    handler = MeteoHandler(config)
    data = handler.query('SBR', ['103'], datetime(2025, 10, 1), datetime(2025, 10, 2), resampler = MeteoResampler(freq='D', min_count = 20))

    data['solar_radiation'].plot()
    print(data)
