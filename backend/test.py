import pandas as pd
from db.conn2 import conn
import asyncio
from sqlalchemy.orm import Session
from db.conn import get_db
from model.PromisedDate_SerialStatus_model import PromisedDateSerialStatus
from fastapi import APIRouter, Depends, HTTPException

SupplyNew ="Driver=SQL Server;Server=AZRSUPPLYDB01\\SUPPLYPLANING;Database=SUPPLYPLANNING_NEW;UID=pyapp;PWD=pwd-querys##2024;"
polypm = "Driver=SQL Server;Server=10.28.1.8\\POLYSQL2019;Database=Tegra_Production;UID=svc_polyreporting;PWD=P0lyPM2021!!;"


#consultasql
textsql="""

SELECT 
    PD.IDUser,
    PD.PurchaseOrder,
    PD.StyleNumber,
    PD.PromisedDate,
    GSP.Process AS SubProcess,
    GP.Process,
    PD_SUB.MO,
    PD_SUB.CuttingNum,
    PD_SUB.SerialNumber,
    PD_SUB.OriginalPromisedDateProcess,
    PD_SUB.PromisedDateProcess,
    PD_SUB.AdjustedPromisedDateProcess,
    PD_SUB.AssignedDateProcess
FROM PromisedDate AS PD
LEFT OUTER JOIN PromisedDate_Cutting AS PD_SUB ON PD.IDPromisedDate = PD_SUB.IDPromisedDate
LEFT OUTER JOIN General_SubProcess AS GSP ON PD_SUB.IDSubProcess = GSP.IDSubProcess
LEFT OUTER JOIN General_Process AS GP ON GSP.IDProcess = GP.IDProcess
WHERE PD.StatusPromisedDate = 1 AND PD_SUB.StatusPromisedDate = 1
and year(PD_SUB.AdjustedPromisedDateProcess)=2025 and MONTH(PD_SUB.AdjustedPromisedDateProcess) in (6,7)
"""

#importando clase
async def get_promised(db: Session = Depends(get_db)):
    db=conn()
    x = await db.connect_and_execute(SupplyNew ,textsql)#metodo asincrono
    db = pd.DataFrame(x)
    print(db)

    items = db.query(PromisedDateSerialStatus).filter(PromisedDateSerialStatus.Status_flag == 1).all()
    db2 = pd.DataFrame(items)
    print(db2)


asyncio.run(get_promised())