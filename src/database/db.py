import datetime
import logging
from contextlib import contextmanager
from pathlib import Path
from typing import Generator, List, Optional, Tuple

import yaml
from sqlalchemy import create_engine
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.orm import Session, sessionmaker
import pandas as pd

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

    def _get_latest_water_balance(
        self, session: Session, field_id: int
    ) -> Optional[models.WaterBalance]:
        return (
            session.query(models.WaterBalance)
            .filter(models.WaterBalance.field_id == field_id)
            .order_by(models.WaterBalance.date.desc())
            .limit(1)
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
    ) -> Tuple[Optional[models.Field], bool]:
        """
        Add a new field or update an existing one.
        """
        reference_station = str(reference_station)
        soil_type = str(soil_type)
        humus_pct = float(humus_pct)
        root_depth_cm = float(root_depth_cm)
        area_ha_value = float(area_ha) if area_ha is not None else None
        p_allowable_value = float(p_allowable) if p_allowable is not None else 0

        updated = False
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
                        return (field, updated)

                session.flush()  # ensure primary key is populated for new records
                return (field, updated)
        except Exception:
            logger.exception("Failed to persist field %s", name)
            return (None, updated)

    def load_fields_from_config(self, config_path: str = "config/fields.yaml") -> bool:
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

        updated_fields = []
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

            field_obj, updated = self.add_field(
                name=field_name,
                reference_station=field_data["reference_station"],
                soil_type=field_data["soil_type"],
                humus_pct=field_data["humus_pct"],
                root_depth_cm=field_data.get("root_depth_cm", 30),
                area_ha=field_data.get("area_ha"),
                p_allowable=field_data.get("p_allowable", 0),
            )
            if updated:
                updated_fields.append(field_obj.id)
        return updated_fields

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

    def query_water_balance(
        self, 
        field_name: str | None = None,
        field_id: int | None = None,
        start: datetime.date | None = None, 
        end: datetime.date | None = None
        ):

        if field_name is not None and field_id is not None:
            raise ValueError("Cannot specify both field_name and field_id")
        
        with self.session_scope() as session:
            query = session.query(models.WaterBalance)

            if field_name is not None:
                field = self._get_field_by_name(session, field_name)
                if field is None:
                    logger.warning("Field %s does not exist. Cannot query water balance", field_name)
                    return []
                query = query.filter(models.WaterBalance.field_id == field.id)

            if field_id is not None:
                query = query.filter(models.WaterBalance.field_id == field_id)

            if start is not None:
                query = query.filter(models.WaterBalance.date >= start)

            if end is not None:
                query = query.filter(models.WaterBalance.date <= end)

            return query.all()

    def latest_water_balance(self, field_id: int) -> Optional[models.WaterBalance]:
        """
        Return the latest water balance entry for a field, or None if absent.
        """
        with self.session_scope() as session:
            return self._get_latest_water_balance(session, field_id)

    def add_water_balance(self, water_balance: pd.DataFrame, field_id: int | None = None):
        """
        Upsert water balance records from a dataframe.
        Returns the number of rows inserted/updated.
        """
        df = water_balance.copy()
        if field_id is not None:
            df["field_id"] = field_id

        required_cols = [
            'field_id',
            'precipitation',
            'irrigation',
            'evapotranspiration',
            'incoming',
            'net',
            'soil_storage',
            'field_capacity',
            'deficit',
        ]
        optional_cols = ['readily_available_water', 'below_raw']

        missing_required = [col for col in required_cols if col not in df.columns]
        if missing_required:
            logger.warning(
                "Not all required columns to save the water balance are present. Missing: %s. "
                "Skipping insertion into database.",
                ", ".join(missing_required),
            )
            return 0

        if not isinstance(df.index, pd.DatetimeIndex):
            logger.warning(
                "Water balance index must be a pandas DatetimeIndex. Got %s. "
                "Skipping insertion into database.",
                type(df.index),
            )
            return 0

        for col in optional_cols:
            if col not in df.columns:
                df[col] = None

        extra_cols = [col for col in df.columns if col not in required_cols + optional_cols]
        if extra_cols:
            logger.info(
                "Additional columns %s will be ignored when saving the water balance.",
                ", ".join(extra_cols),
            )

        df = df.rename_axis("date").reset_index()
        df["date"] = pd.to_datetime(df["date"]).dt.date
        df = df[["date"] + required_cols + optional_cols]

        records = df.to_dict(orient="records")
        if not records:
            logger.info("Water balance dataframe is empty. Nothing to persist.")
            return 0

        # Use SQLite upsert for performance; fall back to per-row merge for other dialects.
        if self.engine.dialect.name == "sqlite":
            stmt = sqlite_insert(models.WaterBalance).values(records)
            update_cols = {
                col: getattr(stmt.excluded, col)
                for col in required_cols + optional_cols
                if col not in ("field_id", "date")
            }
            stmt = stmt.on_conflict_do_update(
                index_elements=[models.WaterBalance.field_id, models.WaterBalance.date],
                set_=update_cols,
            )

            with self.session_scope() as session:
                result = session.execute(stmt)
                return result.rowcount or 0

        with self.session_scope() as session:
            for record in records:
                session.merge(models.WaterBalance(**record))
            return len(records)

    def clear_water_balance(self, field_ids: list[int] | None = None) -> int:
        """
        Delete water balance entries. If field_ids provided, only delete those.
        Returns number of rows deleted.
        """
        try:
            with self.session_scope() as session:
                query = session.query(models.WaterBalance)
                if field_ids:
                    query = query.filter(models.WaterBalance.field_id.in_(field_ids))
                deleted = query.delete(synchronize_session=False)
                logger.info(
                    "Cleared %s water balance rows%s",
                    deleted,
                    f" for fields {field_ids}" if field_ids else "",
                )
                return deleted
        except Exception:
            logger.exception("Failed to clear water balance data")
            return 0

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

    fields = db.get_all_fields()

    for date in pd.date_range("04-01-2025", "10-01-2025", freq = "2W"):
        for field in fields:
            db.add_irrigation_event(
                field_name=field.name,
                date=date.date(),
                method='drip',
            )

    print('Fields in database:')
    print(db.get_all_fields())

    db.close()
