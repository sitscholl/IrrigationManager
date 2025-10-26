from .database.models import Field

class MeteoHandler:
    """Manager class to query meteo data from multiple fields/stations and transform returned data to a consistent schema"""
    
    def __init__(self, config):
        self.api_host = config.api_host

    def query(self, fields: list[Field]):

        for field in fields:
            station = field.reference_station

            try:
                df = self.get_data(station)
            except Exception as e:
                logger.error(f"Error in fetching data for station {station}: {e}")
                return pd.DataFrame()