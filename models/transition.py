from pydantic import BaseModel
from typing import Optional
from datetime import datetime
import json

class TransitionRequest(BaseModel):
    dataAppCode: str
    fromState: str
    toState: str
    timestamp: Optional[datetime] = None
    berthId: Optional[int] = None
    orgId: Optional[int] = None
    
    class Config:
        json_encoders = {
            datetime: lambda dt: dt.isoformat() if dt else None
        }