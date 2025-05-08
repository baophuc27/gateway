# models/data_app.py
from pydantic import BaseModel
from typing import Optional, Dict, List
from datetime import datetime
from datetime import datetime
from sqlalchemy import create_engine, Column, Integer, String, DateTime
from sqlalchemy.ext.declarative import declarative_base

class DataAppHeartbeat(BaseModel):
    code: str
    status: str
    timestamp: float
    last_config_timestamp: Optional[float] = 0

class HeartbeatRequest(BaseModel):
    code: str


class ConfigUpdate(BaseModel):
    config: Dict
    timestamp: float
    version: int

Base = declarative_base()

class DataApp(Base):
    __tablename__ = 'DataApp'
    __table_args__ = {'schema': 'bas'}
    
    code = Column(String(6), primary_key=True)
    orgId = Column(Integer, nullable=False)
    berthId = Column(Integer)
    status = Column(String(255))
    displayName = Column(String(255))
    lastHeartbeat = Column(DateTime(timezone=True))
    lastDataActive = Column(DateTime(timezone=True))
    createdAt = Column(DateTime(timezone=True))