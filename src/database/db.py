import logging
from contextlib import contextmanager
from typing import Generator, List, Optional

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

    def get_all_fields(self) -> List[str]:
        """
        Return the distinct field names sorted alphabetically.
        """
        with self._session_factory() as session:
            names = (
                session.query(models.Field.name)
                .distinct()
                .order_by(models.Field.name)
                .all()
            )
        return [name for (name,) in names]

    def query_field(self, name: str) -> Optional[models.Field]:
        """
        Retrieve a field by its unique name.
        """
        with self._session_factory() as session:
            field = session.query(models.Field).filter(models.Field.name == name).one_or_none()
        return field

    def add_field(
        self,
        name: str,
        reference_station: str,
        soil_type: str,
        area_ha: float,
        p_allowable: float = 0,
    ) -> Optional[models.Field]:
        """
        Add a new field or update an existing one.
        """
        try:
            with self.session_scope() as session:
                field = (
                    session.query(models.Field)
                    .filter(models.Field.name == name)
                    .one_or_none()
                )

                if field is None:
                    logger.debug("Adding new field %s", name)
                    field = models.Field(
                        name=name,
                        reference_station=reference_station,
                        soil_type=soil_type,
                        area_ha=area_ha,
                        p_allowable=p_allowable,
                    )
                    session.add(field)
                else:
                    logger.debug("Updating existing field %s", name)
                    field.reference_station = reference_station
                    field.soil_type = soil_type
                    field.area_ha = area_ha
                    field.p_allowable = p_allowable

                session.flush()  # ensure primary key is populated for new records
                return field
        except Exception as exc:
            logger.exception("Failed to persist field %s: %s", name, exc)
            return None

    def close(self) -> None:
        """
        Dispose of the database engine connection pool.
        """
        if self.engine:
            self.engine.dispose()
            logger.debug("Database engine disposed.")
