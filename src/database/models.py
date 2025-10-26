from sqlalchemy import (
    Column, Integer, Float, String, ForeignKey, DateTime, Text, UniqueConstraint
)
from sqlalchemy.dialects.sqlite import JSON
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base()

class Field(Base):
    __tablename__ = 'fields'

    id = Column(Integer, primary_key = True)
    name = Column(String, nullable = False, unique = True)
    reference_station = Column(String, nullable = False)
    soil_type = Column(String, nullable = False)
    area_ha = Column(Float, nullable = False)
    p_allowable = Column(Float, nullable = False, default = 0)

class Irrigation(Base):
    __tablename__ = 'irrigation_events'
    
    id = Column(Integer, primary_key = True)
    field_id = Column(str, ForeignKey("fields.id"), nullable = False)
    date = Column(DateTime, nullable = False)
    method = Column(String, nullable = False)
    amount = Column(Float, default = 100)

    field = relationship(Field, back_populates = 'irrigation_events')

    __table_args__ = (UniqueConstraint("field_id", "date"), )