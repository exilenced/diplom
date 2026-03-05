from sqlalchemy import Column, String, Integer, Float, DateTime, Date, ForeignKey, DECIMAL
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from datetime import datetime

Base = declarative_base()

class Driver(Base):
    __tablename__ = 'drivers'

    driver_id = Column(String, primary_key=True)  # UUID как строка
    name = Column(String, nullable=False)
    phone = Column(String, unique=True, nullable=False)
    city = Column(String)
    registration_date = Column(DateTime, default=datetime.now)
    status = Column(String, default='active')
    rating = Column(DECIMAL(3,2))
    total_trips = Column(Integer, default=0)

    trips = relationship("Trip", back_populates="driver")
    activities = relationship("DriverActivity", back_populates="driver")

class Trip(Base):
    __tablename__ = 'trips'
    
    trip_id = Column(String, primary_key=True)
    driver_id = Column(String, ForeignKey('drivers.driver_id'))
    trip_date = Column(DateTime)
    city = Column(String)
    distance_km = Column(DECIMAL(10,2))
    duration_min = Column(Integer)
    fare_amount = Column(DECIMAL(10,2))
    commission = Column(DECIMAL(10,2))
    driver_payout = Column(DECIMAL(10,2))
    rating = Column(DECIMAL(2,1))

    driver = relationship("Driver", back_populates="trips")

class DriverActivity(Base):
    __tablename__ = 'driver_activity'
    
    activity_id = Column(String, primary_key=True)
    driver_id = Column(String, ForeignKey('drivers.driver_id'))
    date = Column(Date)
    trips_count = Column(Integer)
    online_hours = Column(DECIMAL(5,2))
    earnings = Column(DECIMAL(10,2))
    accepted_orders = Column(Integer)
    rejected_orders = Column(Integer)

    driver = relationship("Driver", back_populates="activities")