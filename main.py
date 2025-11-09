#Example for base app classes structure: https://github.com/kthorp/pyfao56/blob/main/tests/test01/cottondry2013.py
from src.config import load_config
from src.database.db import IrrigDB
from src.meteo import MeteoHandler
from src.resample import MeteoResampler
from src.et0.base import BaseET0Calculator
from src.et_correction import ETCorrection
from src.plot import WaterBalancePlot


def main():
    
    config = load_config('config.yaml')
    db = IrrigDB(**config['database'])

    fields = db.get_all_fields() #should be a list of FieldHandler instances

    meteo = MeteoHandler(config['meteo_data'])
    resampler = MeteoResampler(config['resampling'])
    et_corrector = ETCorrection(config['et_correction'])
    et_calculator = BaseET0Calculator(config['et0'], corrector = et_corrector)

    wb_plot = WaterBalancePlot(config['plot'])

    reference_stations = Fields.get_reference_stations()
    meteo.query(
        provider = "SBR",
        station_ids = reference_stations,
        start = start,
        end = end,
        resampler = resampler
    )
    meteo.calculate_et0(et_calculator, correct = True)

    for field in fields:
        field_irrigation_events = db.query_irrigation_event(field.name)
        field.calculate_water_balance(
            meteo.get_station_data(field.reference_station), field_irrigation_events
            )

        wb_plot.plot_field(field, field_irrigation_events)



if __name__ == "__main__":
    main()
