import pandas as pd

from dataclasses import dataclass
import logging
import sys
from datetime import datetime, timedelta

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

        self.plot = BasePlot().create_base(vertical_spacing = .1)

        logger.info(f'Initialized WaterBalanceWorkflow with {len(self.fields)} fields from {self.season_start} to {self.season_end}.')

    def run(self):

        for field in self.fields:

            ## Check existing data
            latest_balance = self.db.latest_water_balance(field.id)
            next_date = pd.to_datetime(latest_balance.date) + timedelta(days=1) if latest_balance else self.season_start
            start_date = max(self.season_start, next_date)
            initial_storage = latest_balance.soil_storage if latest_balance else None
            period_end = min(pd.Timestamp.today(), self.season_end)

            if start_date > period_end:
                logger.info(f"No new period to compute for field {field.name}. Latest date in DB: {latest_balance.date if latest_balance else 'none'}.")
            else:
                try:
                    logger.info(f"Found existing data for field {field.name}. Starting calculation from {start_date.date()}")

                    ## Query Meteo Data
                    reference_station = field.reference_station
                    station = self.runtime_context.meteo_handler.query(
                        provider = "SBR",
                        station_ids = reference_station,
                        start = start_date,
                        end = period_end,
                        resampler = self.runtime_context.resampler
                    )

                    if station is None:
                        logger.info(f"No meteo data available from {start_date.date()} for field {field.name}; skipping.")
                        continue

                    ## Calculate evapotranspiration
                    station.data = station.data.join(self.runtime_context.et_calculator.calculate(station, correct = True))

                    ## Calculate water balance
                    field_capacity = field.get_field_capacity()
                    field_irrigation = FieldIrrigation.from_list(self.db.query_irrigation_events(field.name))

                    field_wb = field.calculate_water_balance(
                        station, field_irrigation, initial_storage=initial_storage
                        )
                    
                    ## Persist water balance
                    try:
                        updated_rows = self.db.add_water_balance(field_wb, field_id = field.id)
                    except Exception as e:
                        logger.error(f"Error saving water balance for field {field.name}: {e}")

                    ## Plot
                    self.plot.plot_line(field_wb.index, field_wb["soil_storage"], name=field.name)
                    
                    logger.debug(f"Calculated water-balance for field {field.name}")
                except Exception as e:
                    logger.error(f"Error calculating water balance for field {field.name}: {e}")
                    continue

            
