import datetime
import logging
from contextlib import contextmanager
from typing import Generator, List, Optional, Tuple

from sqlalchemy import create_engine
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.orm import Session, sessionmaker
import pandas as pd

from . import models

logger = logging.getLogger(__name__)


class IrrigDB:
    def __init__(self, path: str = 'sqlite:///database.db', **engine_kwargs) -> None:
        """
        Create a database engine and initialise ORM metadata.
        """
        self.engine = create_engine(path, future=True, **engine_kwargs)
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

    def _query_field(self, session: Session, name: str | None = None, id: int | None = None) -> Optional[models.Field]:

        if name is None and id is None:
            raise ValueError('Cannot query field when id and name are both None')
        if name is not None and id is not None:
            raise ValueError('Cannot query field when both id and name are provided')

        if name is not None:
            return (
                session.query(models.Field)
                .filter(models.Field.name == name)
                .one_or_none()
            )
        elif id is not None:
            return (
                session.query(models.Field)
                .filter(models.Field.id == id)
                .one_or_none()
            )

    def _get_field_by_id(self, session: Session, id: int) -> Optional[models.Field]:
        return (
            session.query(models.Field)
            .filter(models.Field.id == id)
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

    def _get_first_irrigation_event(
        self, session: Session, field_id: int, year: int
    ) -> Optional[models.Irrigation]:
        return (
            session.query(models.Irrigation)
            .filter(models.Irrigation.field_id == field_id)
            .filter(models.Irrigation.date >= datetime.date(year, 1, 1), models.Irrigation.date < datetime.date(year+1, 1, 1))
            .order_by(models.Irrigation.date.asc())
            .limit(1)
            .one_or_none()
        )

    def _get_irrigation_events(
        self, session: Session, field_id: int | None = None, date: datetime.date | None = None, year: int | None = None
    ) -> Optional[models.Irrigation]:

        if date is not None and year is not None:
            logger.warning("Both date and year passed to query_irrigation_events. Ignoring year")
            year = None

        query = session.query(models.Irrigation)
        if field_id is not None:
            query = query.filter(models.Irrigation.field_id == field_id)

        if date is not None:
            query = query.filter(models.Irrigation.date == date)

        if year is not None:
            query = query.filter(models.Irrigation.date >= datetime.date(year, 1, 1), models.Irrigation.date < datetime.date(year+1, 1, 1))

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

    def query_field(self, name: str | None = None, id: int | None = None) -> Optional[models.Field]:
        """
        Retrieve a field by its unique name or its id.
        """
        if name is None and id is None:
            raise ValueError('Cannot query field when id and name are both None')
        if name is not None and id is not None:
            raise ValueError('Cannot query field when both id and name are provided')

        with self.session_scope() as session:
            return self._query_field(session, name = name, id = id)

    def add_field(
        self,
        name: str,
        reference_station: str,
        soil_type: str,
        humus_pct: float,
        root_depth_cm: float = 30,
        area_ha: float | None = None,
        p_allowable: float | None = 0,
        **kwargs #swallow id when created via dashboard
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
                field = self._query_field(session, name = name)

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
                    else:
                        logger.info(f"Updated field {field.name}. Deleting existing water-balance cache")
                        deleted = self._clear_water_balance(session, field_id = field.id)

                session.flush()  # ensure primary key is populated for new records
                return (field, updated)
        except Exception:
            logger.exception("Failed to persist field %s", name)
            return (None, updated)

    def query_irrigation_events(
        self, field_name: str | None = None, date: datetime.date | None = None, year: int | None = None
    ) -> Optional[models.Irrigation]:
        """
        Retrieve an irrigation event by field name and (optional) date.
        """
        if date is not None and isinstance(date, datetime.datetime):
            raise NotImplementedError(
                'Only datetime.date objects are allowed in irrigation database'
            )

        with self.session_scope() as session:
            if field_name is not None:
                field = self._query_field(session, name = field_name)
                if field is None:
                    logger.warning(
                        "Field %s does not exist. Cannot query irrigation event",
                        field_name,
                    )
                    return None
                field_id = field.id
            else:
                field_id = None

            return self._get_irrigation_events(session, field_id, date, year)

    def add_irrigation_event(
        self,
        field_name: str,
        date: datetime.date,
        method: str,
        amount: float = 100,
        id: int | None = None,
    ) -> models.Irrigation:
        
        if isinstance(date, str):
            date = pd.to_datetime(date).date()

        with self.session_scope() as session:
            field = self._query_field(session, name = field_name)
            if field is None:
                raise ValueError(f"Field '{field_name}' not found")

            # Logic: If ID is provided, update that specific row.
            # If no ID, try to find by date/field (legacy logic) or create new.
            event = None
            
            if id is not None:
                event = session.get(models.Irrigation, id)
                if not event:
                     raise ValueError(f"Irrigation event {id} not found")
            
            # If no ID provided, check if one exists for this date/field (prevent duplicates)
            if event is None:
                existing = self._get_irrigation_events(session, field.id, date)
                if existing:
                    event = existing[0]
            old_field_id = event.field_id if event else None

            if event:
                logger.debug("Updating irrigation event %s", event.id)
                event.field_id = field.id 
                event.date = date
                event.method = method
                event.amount = amount
            else:
                logger.debug("Creating new irrigation event")
                event = models.Irrigation(
                    field_id=field.id,
                    date=date,
                    method=method,
                    amount=amount,
                )
                session.add(event)

            self._clear_water_balance(session, field_id = field.id)
            if old_field_id and old_field_id != field.id:
                self._clear_water_balance(session, field_id=old_field_id)

            session.flush()
            # Refresh to ensure we have the ID available if we need to return it
            session.refresh(event) 
            return event

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
                field = self._query_field(session, name = field_name)
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

    def first_irrigation_event(self, field_id: int, year: int):
        with self.session_scope() as session:
            return self._get_first_irrigation_event(session, field_id, year)

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
        # Ensure the series is in UTC before extracting the date
        if df["date"].dt.tz is not None:
            # If it's timezone-aware, convert to UTC
            df["date"] = df["date"].dt.tz_convert("UTC")
        else:
            # If it's naive, assume it's UTC
            df["date"] = df["date"].dt.tz_localize("UTC")
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

    def _clear_water_balance(self, session: Session, field_id: int):
        query = session.query(models.WaterBalance).filter(models.WaterBalance.field_id == field_id)
        deleted = query.delete(synchronize_session=False)
        logger.info(f"Cleared {deleted} water balance rows for field {field_id}")
        return deleted

    def clear_water_balance(self, field_ids: list[int] | None = None) -> int:
        """
        Delete water balance entries. If field_ids provided, only delete those.
        Returns number of rows deleted.
        """

        if field_ids is None:
            with self.session_scope() as session:
                query = session.query(models.WaterBalance)
                deleted = query.delete(synchronize_session=False)
                logger.info(f"Cleared entire water balance cache: {deleted} rows.")
                return deleted

        if isinstance(field_ids, int):
            field_ids = [field_ids]

        deleted_total = 0
        for field_id in field_ids:
            with self.session_scope() as session:
                deleted = self._clear_water_balance(session, field_id)
                deleted_total += deleted
        return deleted_total

    def delete_field(self, field_id: int) -> bool:
        with self.session_scope() as session:
            field = session.get(models.Field, field_id)
            if not field:
                return False
            session.delete(field)
            return True

    def delete_irrigation_event(self, event_id: int) -> bool:
        with self.session_scope() as session:
            event = session.get(models.Irrigation, event_id)
            if not event:
                return False
            session.delete(event)
            self._clear_water_balance(session, field_id = event.field_id)
            return True

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
