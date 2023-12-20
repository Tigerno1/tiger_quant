
from sqlalchemy import Column, Integer, String
from sqlalchemy import Column, Integer, String, Float
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class Kdata(Base):
    __tablename__ = 'kdata'

    id = Column(Integer, primary_key=True)
    symbol = Column(String(length=32))
    open = Column(Float)
    high = Column(Float)
    low = Column(Float)
    close = Column(Float)
    volume = Column(Integer)