import pandas as pd

from dataclasses import dataclass
import logging
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

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
        tz_name = config.get("general", {}).get("timezone", "UTC")
        self.tz = ZoneInfo(tz_name)

        local_now = datetime.now(self.tz)
        self.year = local_now.year

        self.season_end_local = datetime(self.year + 1, 1, 1, tzinfo=self.tz)  # end-exclusive
        self.season_end_utc = self.season_end_local.astimezone(ZoneInfo("UTC"))

        self.db = db

        if ET0Calculator.get_calculator_by_name(config['evapotranspiration']['method']) is None:
            raise ValueError(f"ET0 calculator {config['evapotranspiration']['method']} not found. Choose one of {ET0Calculator.registry.keys()}")

        self.fields = [FieldHandler(field) for field in db.get_all_fields()]

        if len(self.fields) == 0:
            logger.warning('No fields found in database.')
            # logger.info('No fields configured in database. Terminating')
            # sys.exit(1)

        meteo = MeteoHandler(config['meteo'])
        resampler = MeteoResampler(**config['resampling'])
        et_corrector = ETCorrection(**config['evapotranspiration']['correction'])
        et_calculator = ET0Calculator.get_calculator_by_name(config['evapotranspiration']['method'])(corrector = et_corrector)

        self.runtime_context = RuntimeContext(
            meteo_handler = meteo,
            resampler = resampler,
            et_calculator = et_calculator,
        )

        self.plot = BasePlot().create_base(subpanels=0, vertical_spacing=.1)

        logger.info(f'Initialized WaterBalanceWorkflow with {len(self.fields)} fields for year {self.year}.')

    def run(self):

        for field in self.fields:

            field_season_start = self.db.first_irrigation_event(field.id, self.year)

            if field_season_start is None:
                logger.info(f"No irrigation events found for field {field.name}. Skipping")
                continue

            ## Check existing data
            latest_balance = self.db.latest_water_balance(field.id)
            next_date = pd.to_datetime(latest_balance.date) + timedelta(days=1) if latest_balance else field_season_start.date
            start_date = max(field_season_start.date, next_date.date() if hasattr(next_date, "date") else next_date)
            start_ts = pd.Timestamp(start_date, tz="UTC")  #make timezone aware for meteo_handler.query() and for comparison with period_end
            initial_storage = latest_balance.soil_storage if latest_balance else None
            period_end = min(pd.Timestamp.now(tz = self.tz).tz_convert('UTC'), self.season_end_utc)

            if start_ts >= period_end:
                logger.info(f"No new period to compute for field {field.name}. Latest date in DB: {latest_balance.date if latest_balance else 'none'}.")
                wb_persisted = self.db.query_water_balance(
                    field_id = field.id, 
                    start = field_season_start.date, 
                    end = (self.season_end_local - timedelta(days=1)).date() #subtract one day because end-exclusive
                    )
                if wb_persisted:
                    wb_df = pd.DataFrame(
                        [
                            {
                                "date": rec.date,
                                "soil_storage": rec.soil_storage,
                                "irrigation": getattr(rec, "irrigation", 0.0),
                                "precipitation": getattr(rec, "precipitation", 0.0),
                            }
                            for rec in wb_persisted
                        ]
                    )
                    wb_df["date"] = pd.to_datetime(wb_df["date"])
                    wb_df["irrigation"] = wb_df["irrigation"].fillna(0.0)
                    wb_df["precipitation"] = wb_df["precipitation"].fillna(0.0)
                    wb_df = wb_df.set_index("date").sort_index()
                    self.plot.plot_waterbalance(wb_df, field_name=field.name)

                else:
                    logger.info(f"No persisted water balance found for field {field.name}; nothing to plot.")
            else:
                try:
                    logger.info(f"Starting calculation from {start_date} for field {field.name}")

                    ## Query Meteo Data
                    reference_station = field.reference_station
                    station = self.runtime_context.meteo_handler.query(
                        provider = "SBR",
                        station_id = reference_station,
                        start = start_ts,
                        end = period_end,
                        resampler = self.runtime_context.resampler
                    )

                    if station is None:
                        logger.info(f"No meteo data available from {start_date} for field {field.name}; skipping.")
                        continue

                    ## Calculate evapotranspiration
                    station.data = station.data.join(self.runtime_context.et_calculator.calculate(station, correct = True))

                    ## Calculate water balance
                    field_capacity = field.get_field_capacity()
                    field_irrigation = FieldIrrigation.from_list(self.db.query_irrigation_events(field.name, year = self.year))

                    field_wb = field.calculate_water_balance(
                        station.data, field_irrigation, initial_storage=initial_storage
                        )
                    
                    ## Persist water balance
                    try:
                        updated_rows = self.db.add_water_balance(field_wb, field_id = field.id)
                    except Exception as e:
                        logger.error(f"Error saving water balance for field {field.name}: {e}")

                    ## Plot
                    self.plot.plot_waterbalance(field_wb, field_name=field.name)

                    logger.info(f"Calculated water-balance for field {field.name}")
                except Exception as e:
                    logger.error(f"Error calculating water balance for field {field.name}: {e}", exc_info = True)
                    continue

            
