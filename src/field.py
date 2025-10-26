from database.models import Field


class FieldHandler:

    def __init__(self, field: Field, irrigation_manager):
        self.id = field.id
        self.name = field.name
        self.reference_station = field.reference_station
        self.soil_type = field.soil_type
        self.area_ha = field.area_ha
        self.p_allowable = field.p_allowable

    def get_field_capacity(self):
        pass

    def get_irrigation_events(self):
        pass

    def calculate_evapotranspiration(self, meteo_data):
        pass

    def calculate_water_balance(self):
        pass