import pandas as pd

from dataclasses import dataclass
import logging
import sys
from datetime import datetime

from .field import FieldHandler
from .irrigation import FieldIrrigation
from .meteo import MeteoHandler
from .resample import MeteoResampler
from .et0.base import ET0Calculator
from .et_correction import ETCorrection
from .base_plot import BasePlot

logger = logging.getLogger(__name__)

@dataclass
class RuntimeContext:
    meteo_handler: MeteoHandler
    resampler: MeteoResampler
    et_calculator: ET0Calculator

class WaterBalanceWorkflow:

    def __init__(self, config, db):
        self.config = config
        self.season_start = pd.to_datetime(config['general']['season_start'], dayfirst = True)
        self.season_end = pd.to_datetime(config['general']['season_end'], dayfirst = True)
        self.db = db

        if ET0Calculator.get_calculator_by_name(config['evapotranspiration']['method']) is None:
            raise ValueError(f"ET0 calculator {config['evapotranspiration']['method']} not found. Choose one of {ET0Calculator.registry.keys()}")

        self.fields = [FieldHandler(field) for field in db.get_all_fields()]

        if len(self.fields) == 0:
            logger.info('No fields configured in database. Terminating')
            sys.exit(1)

        meteo = MeteoHandler(config['meteo'])
        resampler = MeteoResampler(**config['resampling'])
        et_corrector = ETCorrection(**config['evapotranspiration']['correction'])
        et_calculator = ET0Calculator.get_calculator_by_name(config['evapotranspiration']['method'])(corrector = et_corrector)

        self.runtime_context = RuntimeContext(
            meteo_handler = meteo,
            resampler = resampler,
            et_calculator = et_calculator,
        )

        self.plot = BasePlot().create_base(subpanels=1, vertical_spacing = .1, main_title="Microclimate & Irrigation")

        logger.info(f'Initialized WaterBalanceWorkflow with {len(self.fields)} fields from {self.season_start} to {self.season_end}.')

    def run(self):

        ## Query Meteo Data
        reference_stations = set([i.reference_station for i in self.fields])
        stations = self.runtime_context.meteo_handler.query(
            provider = "SBR",
            station_ids = reference_stations,
            start = self.season_start,
            end = min(datetime.today(), self.season_end),
            resampler = self.runtime_context.resampler
        )

        ## Calculate evapotranspiration
        for st in stations:
            st.data = st.data.join(self.runtime_context.et_calculator.calculate(st, correct = True))

        ## Calculate water balance for each field
        for field in self.fields:
            try:
                station = [i for i in stations if i.id == field.reference_station]
                if len(station) == 0:
                    logger.warning(f"Reference station {field.reference_station} for field {field.name} not found. Skipping")
                    continue

                station = station[0]

                field_capacity = field.get_field_capacity() #todo change humus_pct
                field_irrigation = FieldIrrigation.from_list(self.db.query_irrigation_events(field.name))

                field_wb = field.calculate_water_balance(
                    station.data, field_irrigation
                    )

                self.plot.plot_line(field_wb.index, field_wb["soil_storage"], name=field.name)
                logger.debug(f"Calculated water-balance for field {field.name}")
            except Exception as e:
                logger.error(f"Error calculating water balance for field {field.name}: {e}")
                continue

            
