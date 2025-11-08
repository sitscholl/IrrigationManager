from scipy.stats import mode

import logging
from typing import str, Callable

logger = logging.getLogger(__name__)

def get_mode(column):
    return mode(column, nan_policy='omit').mode[0]

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
            config = DEFAULT_RESAMPLING_CONFIG.update(config)
        
        self.freq = freq
        self.config = config

    def resample(self, meteo_data, default_aggfunc: str | Callable | None = None):

        data_copy = meteo_data.copy()
        resample_colmap = self.resample_colmap.copy()

        missing_columns = [col for col in data_copy.columns if col not in self.config]

        if default_aggfunc is None:
            logger.info(f"The following columns are missing from the resample_colmap: {missing_columns} and no default aggfunc specified. They are ignored from resampling")
            data_copy.drop(missing_columns, axis = 1, inplace = True)
        else:
            for i in missing_columns:
                resample_colmap[i] = default_aggfunc
                
        return data_copy.resample(self.freq).agg({i:j for i,j in resample_colmap.items() if i in data_copy.columns})