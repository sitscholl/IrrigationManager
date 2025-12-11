#Example for base app classes structure: https://github.com/kthorp/pyfao56/blob/main/tests/test01/cottondry2013.py
import logging
import logging.config
import sys

from src.config import load_config
from src.database.db import IrrigDB
from src.field import FieldHandler
from src.meteo import MeteoHandler
from src.resample import MeteoResampler
from src.et0.base import ET0Calculator
from src.et_correction import ETCorrection
from src.base_plot import BasePlot

logger = logging.getLogger(__name__)

def main():
    
    config = load_config('config/config.yaml')
    logging.config.dictConfig(config['logging'])
    db = IrrigDB(**config.get('database', {}))

    if ET0Calculator.get_calculator_by_name(config['evapotranspiration']['method']) is None:
        raise ValueError(f"ET0 calculator {config['evapotranspiration']['method']} not found. Choose one of {ET0Calculator.registry.keys()}")

    fields = [FieldHandler(field) for field in db.get_all_fields()]

    if len(fields) == 0:
        logger.info('No fields configured in database. Terminating')
        sys.exit(1)

    meteo = MeteoHandler(config['meteo'])
    resampler = MeteoResampler(**config['resampling'])
    et_corrector = ETCorrection(**config['evapotranspiration']['correction'])
    et_calculator = ET0Calculator.get_calculator_by_name(config['evapotranspiration']['method'])(corrector = et_corrector)

    wb_plot = BasePlot().create_base(subpanels=1, vertical_spacing = .1, main_title="Microclimate & Irrigation")

    reference_stations = set([i.reference_station for i in fields])
    meteo.query(
        provider = "SBR",
        station_ids = reference_stations,
        start = config['general']['season_start'],
        end = config['general']['season_end'],
        resampler = resampler
    )
    meteo.calculate_et(et_calculator, correct = True)

    for field in fields:
        field_irrigation_events = db.query_irrigation_event(field.name)
        field.calculate_water_balance(
            meteo.get_station_data(field.reference_station), field_irrigation_events
            )

        wb_plot.plot_field(field, field_irrigation_events)



if __name__ == "__main__":
    main()
