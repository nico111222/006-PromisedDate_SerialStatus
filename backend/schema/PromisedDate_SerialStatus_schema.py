from pydantic import BaseModel
from datetime import datetime
from typing import Optional

class PromisedDateSerialStatusBase(BaseModel):
    Serial: int 
    StatusMO: str
    Process: str
    SubProcess: str

    Max_Date_IN: Optional[datetime]
    Min_Date_IN: Optional[datetime]
    Qty_IN: Optional[int]
    Count_IN: Optional[int]

    Max_Date_ON: Optional[datetime]
    Min_Date_ON: Optional[datetime]
    Qty_ON: Optional[int]
    Count_ON: Optional[int]

    Max_Date_OUT: Optional[datetime]
    Min_Date_OUT: Optional[datetime]
    Qty_OUT: Optional[int]
    Count_OUT: Optional[int]
    Status_flag :  int = 1

class PromisedDateSerialStatusCreate(PromisedDateSerialStatusBase):
    pass

class PromisedDateSerialStatusUpdate(PromisedDateSerialStatusBase):
    Serial: int 
    StatusMO: str
    Process: str
    SubProcess: str

    Max_Date_IN: Optional[datetime]
    Min_Date_IN: Optional[datetime]
    Qty_IN: Optional[int]
    Count_IN: Optional[int]

    Max_Date_ON: Optional[datetime]
    Min_Date_ON: Optional[datetime]
    Qty_ON: Optional[int]
    Count_ON: Optional[int]

    Max_Date_OUT: Optional[datetime]
    Min_Date_OUT: Optional[datetime]
    Qty_OUT: Optional[int]
    Count_OUT: Optional[int]
    Status_flag :  int = 1

class PromisedDateSerialStatusschema(PromisedDateSerialStatusBase):
    IDStatus: int

    class Config:
        orm_mode = True
