from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import pandas as pd

import logging
from datetime import datetime, timezone

from . import models

logger = logging.getLogger(__name__)

class IrrigDB:

    def __init__(self, engine: str = 'sqlite:///database.db'):
        self.engine = create_engine(engine)
        models.Base.metadata.create_all(self.engine)
        self.session = sessionmaker(bind=self.engine, autocommit = False, autoflush = False)()

    def get_fields(self):
        query = self.session.query(models.Field.name.distinct())
        return query.all()

    def query_field(self, name):
        query = self.session.query(models.Field).filter(models.Field.name == name)
        return query.all()

    def add_field(self, name, reference_station, soil_type, area_ha, p_allowable):
        
        existing_field = self.query_field(name = name)

        ##TODO: Update existing field with new args
        if existing_field:
            return existing_field[0]

        field = models.Field(name = name, reference_station = reference_station, soil_type = soil_type, area_ha = area_ha, p_allowable = p_allowable)
        self.session.add(field)
        self.session.commit()
        return field




    def query_station(self, provider: str | None = None, external_id: str | None = None):
        query = self.session.query(models.Station)
        if provider is not None:
            query = query.filter(models.Station.provider == provider)
        if external_id is not None:
            query = query.filter(models.Station.external_id == external_id)
        return query.all()

    def query_variable(self, name: str = None):
        query = self.session.query(models.Variable)
        if name is not None:
            query = query.filter(models.Variable.name == name)
        return query.all()

    def query_data(
            self,
            provider: str,
            station_id: str,
            start_time: datetime,
            end_time: datetime,
            variables: list[str] | None = None,
        ):

        orig_timezone = start_time.tzinfo
        start_time_utc = start_time.astimezone(timezone.utc)
        end_time_utc = end_time.astimezone(timezone.utc)

        query = (
            self.session.query(
                models.Measurement.datetime.label("datetime"),
                models.Measurement.value.label("value"),
                models.Station.external_id.label("station_id"),
                models.Variable.name.label("variable"),
                models.Station.provider.label("provider"),
            )
            .join(models.Station, models.Measurement.station_id == models.Station.id)
            .join(models.Variable, models.Measurement.variable_id == models.Variable.id)
            .filter(
                models.Station.provider == provider,
                models.Station.external_id == station_id,
                models.Measurement.datetime.between(start_time_utc, end_time_utc)
            )
        )

        if variables is not None:
            variables_ids = []
            for v in variables:
                variable_model = self.query_variable(v)
                if not variable_model:
                    logger.warning(f"Variable {v} not found in database")
                    continue
                variables_ids.append(variable_model[0].id)

            query = query.filter(
                models.Measurement.variable_id.in_(variables_ids)
            )

        df = pd.read_sql_query(sql=query.statement, con=self.engine)

        if not df.empty:
            try:
                df['datetime'] = df['datetime'].dt.tz_localize(timezone.utc).dt.tz_convert(orig_timezone)
            except Exception as e:
                logger.warning(f"Could not convert timezone back to {orig_timezone}: {e}. Keeping UTC timezone.")
                # Ensure index is UTC-aware if conversion fails
                if df['datetime'].tz is None:
                    df['datetime'] = df['datetime'].tz_localize('UTC')

            df = df.pivot(columns = 'variable', values = 'value', index = ['station_id', 'datetime'])
            df.reset_index(level = 0, inplace = True)

        return df

    def insert_station(self, provider: str, external_id: str, **kwargs):
        """
        Get existing station if it already exists or create a new one.
        """
        existing_station = self.query_station(provider=provider, external_id=external_id)

        if existing_station:
            return existing_station[0]

        #Fetch station information
        station_info = None
        if self.provider_manager is None:
            logger.info("ProviderManager is not initialized. Cannot fetch station info")
        elif self.provider_manager.get_provider(provider.lower()) is None:
            logger.warning(f"Provider handler for provider '{provider}' could not be found. Station metadata will not be fetched. Available providers: {self.provider_manager.list_providers()}")
        else:
            try:
                with self.provider_manager.get_provider(provider.lower()) as provider_handler:
                    station_info = provider_handler.get_station_info(external_id)
                    station_info.update(**kwargs)
            except Exception as e:
                logger.error(f"Error fetching station information: {e}")

        if station_info is None:
            station_info = kwargs

        try:
            new_station = models.Station(provider = provider, external_id = external_id, **station_info)
            self.session.add(new_station)
            self.session.commit()
            logger.info(f"New station {new_station.external_id} inserted successfully.")
            self.session.refresh(new_station)
            return new_station
        except Exception as e:
            logger.error(f"Error inserting new station: {e}")
            return

    def insert_variable(self, name: str, unit: str | None = None, description: str | None = None):
        """
        Get existing variable if it already exists or create a new one.
        """
        existing_variable = self.query_variable(name=name)

        if existing_variable:
            return existing_variable[0]

        try:
            new_variable = models.Variable(name = name, unit = unit, description = description)
            self.session.add(new_variable)
            self.session.commit()
            logger.info(f"New variable {new_variable.name} inserted successfully.")
            self.session.refresh(new_variable)
            return new_variable
        except Exception as e: 
            logger.error(f"Error inserting new variable: {e}")
            return

    def insert_data(
        self, 
        data: pd.DataFrame, 
        provider: str,
        index=False, index_label=None, if_exists='append'):
        """
        Insert measurement data into the database. All columns other than 'datetime' and 'station_id' are assumed to contain variables and will be inserted into the Measurement table.
        Stations and variables which do not exist in the database will be created automatically in the respective tables.

        Expected DataFrame columns:
        - 'datetime': Measurement timestamp
        - 'station_id': External id of the station

        Optional attrs for station creation:
        - 'station_name', 'latitude', 'longitude', 'elevation'

        Optional attrs for variable creation:
        - 'variable_unit', 'variable_description'
        """
        if data.empty:
            logger.warning("Empty DataFrame provided to insert_data")
            return

        # Validate required columns
        required_cols = ['datetime', 'station_id']
        missing_cols = [col for col in required_cols if col not in data.columns]
        if missing_cols:
            raise ValueError(f"Missing required columns: {missing_cols}")

        variable_columns = [col for col in data.columns if col not in required_cols]
        if len(variable_columns) == 0:
            logger.warning('No variable columns found in data')
            return

        # Make a copy to avoid modifying the original DataFrame
        df = data.copy()

        for st_id, station_data in df.groupby('station_id'):

            #Get internal station entry and create if it does not exist
            # ##TODO: Query station info from provider class and get all attributes 
            station_entry = self.insert_station(provider, st_id)

            if station_entry is None:
                logger.warning(f"Skipping insertion of data from {st_id} as station could not be inserted into database")
                continue

            for var in variable_columns:
                variable_entry = self.insert_variable(name=var)

                if variable_entry is None:
                    logger.warning(f"Skipping insertion of data from {st_id} and variable {var} as variable could not be inserted into database")
                    continue

                measurements = station_data[['datetime', var]].copy()
                measurements['datetime'] = measurements['datetime'].dt.tz_convert('UTC')
                measurements['station_id'] = station_entry.id
                measurements['variable_id'] = variable_entry.id
                measurements.rename(columns = {var: 'value'}, inplace = True)
                
                try:
                    measurements.to_sql(
                        name='measurements',
                        con=self.engine,
                        index=index,
                        index_label=index_label,
                        if_exists=if_exists
                    )
                    self.session.commit()
                    logger.info(f"Successfully inserted {len(measurements)} measurements values for station {st_id} and variable {var}")
                except Exception as e:
                    self.session.rollback()
                    logger.error(f"Error inserting data for station {st_id} and variable {var}: {e}")
                    
    def close(self):
        """
        Close the SQLAlchemy session and dispose of the engine connection pool.
        """
        try:
            if self.session:
                self.session.close()
                logger.debug("Database session closed.")
        except Exception as e:
            logger.warning(f"Error closing database session: {e}")

        try:
            if self.engine:
                self.engine.dispose()
                logger.debug("Database engine disposed.")
        except Exception as e:
            logger.warning(f"Error disposing database engine: {e}")