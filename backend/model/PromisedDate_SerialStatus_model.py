from sqlalchemy import Column, Integer, String,DateTime,BigInteger
#from sqlalchemy.orm import relationship
from db.conn import Base

class PromisedDateSerialStatus(Base):
    __tablename__ = "PromisedDate_SerialStatus"

    IDStatus = Column(Integer, primary_key=True, index=True)
    Serial = Column(BigInteger, nullable=False) 
    StatusMO = Column(String(50), nullable=False)
    Process = Column(String(50), nullable=False)
    SubProcess = Column(String(50), nullable=False)

    Max_Date_IN = Column(DateTime)
    Min_Date_IN = Column(DateTime)
    Qty_IN = Column(Integer)
    Count_IN = Column(Integer)
    
    Max_Date_ON = Column(DateTime)
    Min_Date_ON = Column(DateTime)
    Qty_ON = Column(Integer)
    Count_ON = Column(Integer)
    
    Max_Date_OUT = Column(DateTime)
    Min_Date_OUT = Column(DateTime)
    Qty_OUT = Column(Integer)
    Count_OUT = Column(Integer)
    Status_flag = Column(Integer, default=1)

