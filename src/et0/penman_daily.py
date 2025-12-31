from pyet import pm_fao56
import pandas as pd

from typing import TYPE_CHECKING
import logging

from .base import ET0Calculator

if TYPE_CHECKING:
    from ..et_correction import ETCorrection
    from ..meteo import Station

logger = logging.getLogger(__name__)

class PenmanDailyCalculator(ET0Calculator):

    def __init__(self, corrector: "ETCorrection | None" = None, **kwargs):
        self.corrector = corrector

    @classmethod
    def name(cls):
        return "PenmanDaily"

    def _validate_data(self, data):
        if not isinstance(data.index, pd.DatetimeIndex):
            raise ValueError(
                f"Index of input data has to be of type datetime. Got {data.index.dtype}"
            )

        freq = None
        try:
            freq = pd.infer_freq(data.index)
        except Exception as e:
            logger.warning(
                "Failed to determine input datetime frequency for PenmanDailyCalculator "
                f"validation with error: {e}"
            )

        if freq is not None and freq != "D":
            raise ValueError(f"Index of input data has to at daily frequency. Got {freq}")

    def calculate(self, station: "Station", correct: bool = True):

        if correct and self.corrector is None:
            raise ValueError('Correct set to true but no corrector available.')

        meteo_data = station.data
        self._validate_data(meteo_data)

        et = pm_fao56(
            meteo_data.tair_2m, 
            meteo_data.wind_speed, 
            rs=meteo_data.solar_radiation,
            elevation=station.elevation, 
            lat=station.latitude, 
            tmax=meteo_data.tair_2m_max, 
            tmin=meteo_data.tair_2m_min, 
            rh=meteo_data.relative_humidity
            )

        et.name = 'et0'
        et = et.to_frame()
        
        if correct:
            et = self.corrector.apply_to(et, "et0")

        return et
