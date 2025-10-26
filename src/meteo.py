import pandas as pd
import requests
import pandera.pandas as pa

from datetime import datetime
import logging

logger = logging.getLogger(__name__)

class MeteoHandler:
    """Manager class to query meteo data from multiple fields/stations and transform returned data to a consistent schema"""
    
    def __init__(self, config):
        self.api_host = config['api']['host']
        self.query_template = config['api']["query_template"]

    @property
    def output_schema(self) -> pa.DataFrameSchema:
        """
        Define the expected schema for meteorological data output.
        
        Returns:
            pa.DataFrameSchema: Schema for validating SBR output data
        """
        return pa.DataFrameSchema(
            {
                "datetime": pa.Column(pd.DatetimeTZDtype(tz="UTC")),
                "station_id": pa.Column(str),

                "tair_2m": pa.Column(float, nullable=True, required = False),
                "relative_humidity": pa.Column(float, nullable=True, required=False),
                "wind_speed": pa.Column(float, nullable=True, required=False),
                "precipitation": pa.Column(float, nullable=True, required = False),  
                "air_pressure": pa.Column(float, nullable = True, required = False),
                "sun_duration": pa.Column(float, nullable = True, required = False),
                "solar_radiation": pa.Column(float, nullable = True, required = False),
            },
            index=pa.Index(int),
            strict=False  # Allow additional columns that might be added
        )

    def _validate(self, transformed_data: pd.DataFrame):
        return self.output_schema.validate(transformed_data)

    def get_data(self, provider: str, station_id: str, start: datetime, end: datetime):
        url = self.api_host + "/" + self.query_template.format(provider = provider, station_id = station_id, start_date = start, end_date = end)
        try:
            response = requests.get(url)
            response.raise_for_status()

            response_data = pd.DataFrame(response.json()['data'])
            response_data['datetime'] = pd.to_datetime(response_data['datetime'])
            

        except requests.exceptions.RequestException as e:
            logger.error(f"Error in fetching data from {url}: {e}")
            return pd.DataFrame()
        
        return response_data


    def query(self, provider: str, station_ids: list[str], start: datetime, end: datetime):

        data = []
        for station in station_ids:

            try:
                df = self.get_data(provider, station, start, end)
                validated_df = self._validate(df)
                data.append(validated_df)
            except Exception as e:
                logger.error(f"Error in fetching data for station {station}: {e}")
                return pd.DataFrame()

        return pd.concat(data, ignore_index = True)

if __name__ == '__main__':
    
    from .config import load_config

    config = load_config('config/config.yaml')

    handler = MeteoHandler(config)
    data = handler.query('SBR', ['103'], datetime(2025,10,1), datetime(2025,10,2))

    print(data)