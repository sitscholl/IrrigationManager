from dataclasses import dataclass

from .database.models import Field
from .meteo import MeteoData
from .irrigation import IrrigationManager

@dataclass
class WaterBalanceCalculator:

    field: Field
    meteo: MeteoData
    irrigation: IrrigationManager

    def _calculate_soil_water_capacity(self):
        pass

    def calculate_water_balance(self):
        pass