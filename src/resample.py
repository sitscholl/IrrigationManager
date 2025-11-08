import pandas as pd

import logging
from typing import Callable

logger = logging.getLogger(__name__)

def get_mode(column: pd.Series):
    return column.mode.iloc[0]

DEFAULT_RESAMPLING_CONFIG = {
    "tair_2m": "mean",                           # Temperature 2m
    "tsoil_25cm": "mean",     # Soil temperature -25cm
    "tdry_60cm": "mean",           # Dry temperature 60cm
    "twet_60cm": "mean",           # Wet temperature
    "relative_humidity": "mean",           # Relative humidity
    "wind_speed": "mean",         # Wind speed
    "wind_gust": "max",      # Max wind gust
    "wind_direction": get_mode,
    "precipitation": "sum",                        # Precipitation
    "irrigation": "max",         # Irrigation
    "leaf_wetness": "mean",           # Leaf wetness
    "air_pressure": "mean",
    "sun_duration": "mean",
    "solar_radiation": "sum",
    "snow_height": "mean",
    "water_level": "mean",
    "discharge": "mean"
    }

class MeteoResampler:

    def __init__(self, freq = 'D', config: dict | None = None):
        
        if config is None:
            config = DEFAULT_RESAMPLING_CONFIG
        else:
            config = {**DEFAULT_RESAMPLING_CONFIG, **config}
        
        self.freq = freq
        self.config = config

    def resample(self, meteo_data, default_aggfunc: str | Callable | None = None):

        data_copy = meteo_data.copy()
        resample_colmap = self.config.copy()

        missing_data_columns = [col for col in resample_colmap if col not in data_copy.columns]
        if missing_data_columns:
            logger.info(
                "Columns configured for resampling are missing in the input data: %s",
                missing_data_columns,
            )
            for col in missing_data_columns:
                resample_colmap.pop(col, None)

        extra_columns = [col for col in data_copy.columns if col not in resample_colmap]
        if extra_columns:
            if default_aggfunc is None:
                logger.info(
                    "Columns %s lack a resampling rule and no default aggfunc was provided; dropping them.",
                    extra_columns,
                )
                data_copy.drop(columns=extra_columns, inplace=True)
            else:
                for col in extra_columns:
                    resample_colmap[col] = default_aggfunc
                
        return data_copy.resample(self.freq).agg({i:j for i,j in resample_colmap.items() if i in data_copy.columns})
