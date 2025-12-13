from sqlalchemy import Column, Date, DateTime, Float, ForeignKey, Integer, String, UniqueConstraint
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base()


class Field(Base):
    __tablename__ = 'fields'

    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False, unique=True)
    reference_station = Column(String, nullable=False)
    soil_type = Column(String, nullable=False)
    humus_pct = Column(Float, nullable = False)
    area_ha = Column(Float, nullable=True)
    root_depth_cm = Column(Float, nullable=False)
    p_allowable = Column(Float, nullable=False, default=0)

    irrigation_events = relationship(
        'Irrigation',
        back_populates='field',
        cascade='all, delete-orphan'
    )

    def __repr__(self) -> str:
        return f"Field(id={self.id!r}, name={self.name!r})"


class Irrigation(Base):
    __tablename__ = 'irrigation_events'

    id = Column(Integer, primary_key=True)
    field_id = Column(Integer, ForeignKey('fields.id'), nullable=False)
    date = Column(Date, nullable=False)
    method = Column(String, nullable=False)
    amount = Column(Float, default=100)

    field = relationship('Field', back_populates='irrigation_events')

    __table_args__ = (UniqueConstraint('field_id', 'date', name='uq_irrigation_field_date'),)

    def __repr__(self) -> str:
        return f"Irrigation(id={self.id!r}, field_id={self.field_id!r}, date={self.date!r})"
