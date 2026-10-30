import datetime  # Import datetime to create timestamps for our incoming data records
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime  # Import SQL data types and components
from sqlalchemy.ext.declarative import declarative_base  # Import tool to build a base class for our database models
from sqlalchemy.orm import sessionmaker  # Import session maker to handle database connection transactions
from fastapi import FastAPI

DATABASE_URL = "sqlite:///file:live_ram_store?mode=memory&cache=shared&uri=true"

engine=create_engine(DATABASE_URL,connect_args={"create_same_thread":False})

sessionLocal = sessionmaker(autoflush=False, autocommit=False,bind=engine)

Base= declarative_base()

class LiveTelemetry(Base):
    __tablename__="live_telemetry"
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    device_id=Column(String,index=True)
    reading=Column(Float)
    status=Column(String)
    timestamp=Column(DateTime,default=datetime.datetime.utcnow)

Base.metadata.create_all(bind=engine)
