import datetime
import logging
from contextlib import contextmanager
from pathlib import Path
from typing import Generator, List, Optional

import yaml
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from . import models

logger = logging.getLogger(__name__)


class IrrigDB:
    def __init__(self, engine_url: str = 'sqlite:///database.db', **engine_kwargs) -> None:
        """
        Create a database engine and initialise ORM metadata.
        """
        self.engine = create_engine(engine_url, future=True, **engine_kwargs)
        models.Base.metadata.create_all(self.engine)
        self._session_factory = sessionmaker(
            bind=self.engine,
            autocommit=False,
            autoflush=False,
            expire_on_commit=False,
            future=True,
        )

    @contextmanager
    def session_scope(self) -> Generator[Session, None, None]:
        """
        Provide a transactional scope around a series of operations.
        """
        session: Session = self._session_factory()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def _get_field_by_name(self, session: Session, name: str) -> Optional[models.Field]:
        return (
            session.query(models.Field)
            .filter(models.Field.name == name)
            .one_or_none()
        )

    def _get_irrigation_events(
        self, session: Session, field_id: int, date: datetime.date | None = None
    ) -> Optional[models.Irrigation]:
        query = (
            session.query(models.Irrigation)
            .filter(
                models.Irrigation.field_id == field_id,
            )
        )

        if date is not None:
            query = query.filter(models.Irrigation.date == date)

        return query.all()


    def get_all_fields(self) -> List[str]:
        """
        Return the distinct field names sorted alphabetically.
        """
        with self.session_scope() as session:
            fields = (
                session.query(models.Field)
                .order_by(models.Field.name)
                .all()
            )
        return fields

    def query_field(self, name: str) -> Optional[models.Field]:
        """
        Retrieve a field by its unique name.
        """
        with self.session_scope() as session:
            return self._get_field_by_name(session, name)

    def add_field(
        self,
        name: str,
        reference_station: str,
        soil_type: str,
        humus_pct: float,
        root_depth_cm: float = 30,
        area_ha: float | None = None,
        p_allowable: float | None = 0,
    ) -> Optional[models.Field]:
        """
        Add a new field or update an existing one.
        """
        reference_station = str(reference_station)
        soil_type = str(soil_type)
        humus_pct = float(humus_pct)
        root_depth_cm = float(root_depth_cm)
        area_ha_value = float(area_ha) if area_ha is not None else None
        p_allowable_value = float(p_allowable) if p_allowable is not None else 0

        try:
            with self.session_scope() as session:
                field = self._get_field_by_name(session, name)

                if field is None:
                    logger.debug("Adding new field %s to database", name)
                    field = models.Field(
                        name=name,
                        reference_station=reference_station,
                        soil_type=soil_type,
                        humus_pct=humus_pct,
                        root_depth_cm=root_depth_cm,
                        area_ha=area_ha_value,
                        p_allowable=p_allowable_value,
                    )
                    session.add(field)
                else:
                    updated = False

                    if field.reference_station != reference_station:
                        field.reference_station = reference_station
                        updated = True
                    if field.soil_type != soil_type:
                        field.soil_type = soil_type
                        updated = True
                    if field.humus_pct != humus_pct:
                        field.humus_pct = humus_pct
                        updated = True
                    if field.root_depth_cm != root_depth_cm:
                        field.root_depth_cm = root_depth_cm
                        updated = True
                    if field.area_ha != area_ha_value:
                        field.area_ha = area_ha_value
                        updated = True
                    if field.p_allowable != p_allowable_value:
                        field.p_allowable = p_allowable_value
                        updated = True

                    if not updated:
                        logger.debug("No changes for field %s; skipping update", name)
                        return field

                session.flush()  # ensure primary key is populated for new records
                return field
        except Exception:
            logger.exception("Failed to persist field %s", name)
            return None

    def load_fields_from_config(self, config_path: str = "config/fields.yaml") -> None:
        """
        Load fields from a YAML file and upsert them into the database.
        """
        config_file = Path(config_path)

        if not config_file.exists():
            logger.warning("Field configuration file %s not found. Skipping field sync.", config_file)
            return

        try:
            with config_file.open("r", encoding="utf-8") as file:
                field_config = yaml.safe_load(file) or {}
        except Exception:
            logger.exception("Failed to read field configuration from %s", config_file)
            return

        if not isinstance(field_config, dict):
            logger.error("Field configuration in %s must be a mapping of field names to attributes.", config_file)
            return

        for field_name, field_data in field_config.items():
            if not isinstance(field_data, dict):
                logger.warning("Skipping field %s because its configuration is not a mapping.", field_name)
                continue

            missing_keys = [key for key in ("reference_station", "humus_pct", "soil_type") if key not in field_data]
            if missing_keys:
                logger.warning(
                    "Skipping field %s because required keys are missing: %s",
                    field_name,
                    ", ".join(missing_keys),
                )
                continue

            self.add_field(
                name=field_name,
                reference_station=field_data["reference_station"],
                soil_type=field_data["soil_type"],
                humus_pct=field_data["humus_pct"],
                root_depth_cm=field_data.get("root_depth_cm", 30),
                area_ha=field_data.get("area_ha"),
                p_allowable=field_data.get("p_allowable", 0),
            )

    def query_irrigation_events(
        self, field_name: str, date: datetime.date | None = None
    ) -> Optional[models.Irrigation]:
        """
        Retrieve an irrigation event by field name and (optional) date.
        """
        if date is not None and isinstance(date, datetime.datetime):
            raise NotImplementedError(
                'Only datetime.date objects are allowed in irrigation database'
            )

        with self.session_scope() as session:
            field = self._get_field_by_name(session, field_name)
            if field is None:
                logger.warning(
                    "Field %s does not exist. Cannot query irrigation event",
                    field_name,
                )
                return None

        return self._get_irrigation_events(session, field.id, date)

    def add_irrigation_event(
        self,
        field_name: str,
        date: datetime.date,
        method: str,
        amount: float = 100,
    ) -> Optional[models.Irrigation]:
        """
        Add a new irrigation event or update an existing one.
        """
        if isinstance(date, datetime.datetime):
            raise NotImplementedError(
                'Only datetime.date objects are allowed in irrigation database'
            )

        try:
            with self.session_scope() as session:
                field = self._get_field_by_name(session, field_name)
                if field is None:
                    logger.error(
                        "Field %s does not exist. Cannot add irrigation event",
                        field_name,
                    )
                    return None

                events = self._get_irrigation_events(session, field.id, date)

                if len(events) == 0:
                    logger.debug(
                        "Adding new irrigation event for field %s on %s to database",
                        field_name,
                        date,
                    )
                    event = models.Irrigation(
                        field_id=field.id,
                        date=date,
                        method=method,
                        amount=amount,
                    )
                    session.add(event)
                else:
                    logger.debug(
                        "Updating irrigation event for field %s on %s",
                        field_name,
                        date,
                    )
                    event = events[0] #TODO: Improve this to also handle the case where multiple events in one day
                    event.method = method
                    event.amount = amount

                session.flush()  # ensure primary key is populated for new records
                return event
        except Exception:
            logger.exception(
                "Failed to add irrigation event for field %s on %s",
                field_name,
                date,
            )
            return None

    def close(self) -> None:
        """
        Dispose of the database engine connection pool.
        """
        if self.engine:
            self.engine.dispose()
            logger.debug("Database engine disposed.")


if __name__ == '__main__':
    import logging.config
    import pandas as pd

    # from ..config import load_config

    # config = load_config('config/config.yaml')
    # logging.config.dictConfig(config['logging'])

    db = IrrigDB()

    fields = {
        'Gänsacker': {'reference_station': '103', 'soil_type': 'sandiger Schluff', 'area_ha': 1},
        'Pignatter': {'reference_station': '103', 'soil_type': 'sandiger Schluff', 'area_ha': 2},
        'Dietlacker': {'reference_station': '103', 'soil_type': 'humoser lehmiger Sand', 'area_ha': 3},
    }

    for field_name in fields.keys():
        db.add_field(
            name=field_name,
            **fields[field_name],
        )

    # db.add_field(
    #     name='Dietlacker',
    #     reference_station='113',
    #     soil_type = 'sandiger Schluff',
    #     area_ha = 4
    # )

    for date in pd.date_range("04-01-2025", "10-01-2025", freq = "2W"):
        for field in fields.keys():
            db.add_irrigation_event(
                field_name=field,
                date=date.date(),
                method='drip',
            )

    print('Fields in database:')
    print(db.get_all_fields())

    print('Irrigation events for Gänsacker:')
    print(db.query_irrigation_events('Gänsacker'))
    db.close()
