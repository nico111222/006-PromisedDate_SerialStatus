from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from db.conn import get_db
from model.PromisedDate_SerialStatus_model import PromisedDateSerialStatus
from schema.PromisedDate_SerialStatus_schema import (
    PromisedDateSerialStatusschema,
    PromisedDateSerialStatusCreate,
    PromisedDateSerialStatusUpdate
)

router = APIRouter(tags=["PromisedDate_SerialStatus"])

@router.get("/get_PromisedDate_SerialStatus", response_model=list[PromisedDateSerialStatusschema])
def get_PromisedDate_SerialStatus(db: Session = Depends(get_db)):
    items = db.query(PromisedDateSerialStatus).filter(PromisedDateSerialStatus.Status_flag == 1).all()
    if not items:
        raise HTTPException(status_code=404, detail="No se encuentran registros activos")
    return items

@router.get("/get_Id_PromisedDate_SerialStatus", response_model=PromisedDateSerialStatusschema)
def get_id_PromisedDate_SerialStatus(id: int, db: Session = Depends(get_db)):
    item = db.query(PromisedDateSerialStatus).filter(PromisedDateSerialStatus.IDStatus == id).first()
    if not item:
        raise HTTPException(status_code=404, detail="No se encuentra el ID")
    return item

@router.post("/create_PromisedDate_SerialStatus", response_model=PromisedDateSerialStatusschema)
def create_PromisedDate_SerialStatus(item: PromisedDateSerialStatusCreate, db: Session = Depends(get_db)):
    db_item = PromisedDateSerialStatus(**item.model_dump())
    db.add(db_item)
    db.commit()
    db.refresh(db_item)
    return db_item

@router.put("/update_PromisedDate_SerialStatus", response_model=PromisedDateSerialStatusschema)
def update_PromisedDate_SerialStatus(id: int, item: PromisedDateSerialStatusUpdate, db: Session = Depends(get_db)):
    db_item = db.query(PromisedDateSerialStatus).filter(PromisedDateSerialStatus.IDStatus == id).first()
    if not db_item:
        raise HTTPException(status_code=404, detail="No se puede actualizar")

    for field, value in item.model_dump(exclude_unset=True).items():
        setattr(db_item, field, value)

    db.commit()
    db.refresh(db_item)
    return db_item

@router.delete("/delete_PromisedDate_SerialStatus", response_model=PromisedDateSerialStatusschema)
def delete_PromisedDate_SerialStatus(id: int, db: Session = Depends(get_db)):
    db_item = db.query(PromisedDateSerialStatus).filter(PromisedDateSerialStatus.IDStatus == id).first()
    if not db_item:
        raise HTTPException(status_code=404, detail="No se puede eliminar el ID")

    db_item.Status_flag = 0  
    db.commit()
    db.refresh(db_item)
    return db_item

#miguel
from db.conn2 import conn
import pandas as pd

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

textsql2 = """
SELECT * FROM (

	SELECT --top 20
			Orders.OrderID
			,CASE WHEN ManufactureOrders.CutNumber IS NULL THEN ManufactureOrders.ManufactureNumber + '-001' ELSE ManufactureOrders.ManufactureNumber + '-' + ManufactureOrders.CutNumber END AS KEY1
			,Addresses.CompanyNumber AS Customer
			,RTrim(Case When ManufactureOrders.ManufactureID<=999999 Then 
							Replicate('0', 6-Len(Cast(ManufactureOrders.ManufactureID As Nchar))) 
						Else '' End
					+ Cast(ManufactureOrders.ManufactureID As Nchar)) As SerialNumber
			,OrderItems.QuantityRequested AS QuantityOrdered
			,Warehouses.WarehouseName AS GoodsWarehouse
			,DropDownValues2.DropDownValue
			,EnumValues.EnumValue
			,Divisions.DivisionName
			,StyleCategories.StyleCategoryName
			,StyleColors.StyleColorDescription
			,'SO-Full Order ' AS TypeOrder
			,Styles.StyleNumber AS Style
			,Seasons.SeasonCode
			,ManufactureOrders.ManufactureNumber AS MO
			,Orders.PONumber As PONumber
			,ISNULL(ManufactureOrders.CutNumber, '001') AS Cut
			,CONVERT(DATE, Orders.OriginalRequiredDate) As OGAC
			,CONVERT(DATE, ManufactureOrders.TargetDate) AS XFactoryDate
			,CONVERT(DATE, Orders.RequiredDate) AS GACDate
			,CONVERT(DATE,Shipments2.ShipDate) As ShipDate
			,ISNULL(Orders.ShipCount, 0) AS ShipCount
			,CONVERT(DATE, ManufactureOrders.WithdrawDateB1) As PlanMaterialDate
			,CONVERT(DATE, ManufactureOrders.MaterialDate) AS CurrentPRDFabric
			,CONVERT(DATE, ManufactureOrders.TrimMaterialDate)  AS CurrentPRDTrim
			,CONVERT(DATE, ManufactureOrders.SuppliesMaterialDate) AS CurrentPRDSupplies
			,Orders.Comments2 AS BuyMonthYear
			,Styles.StyleName AS StyleName
			,Styles.Comments3 AS StyleComments3
			,StyleColors.StyleColorName AS Color
			,Orders.Comments6 AS OrderComments6
			,Orders.Comments3
			,ISNULL(Orders.RequestCount, 0) AS CustomerQty
			,ManufactureOrders.OrderDate AS MarkerDate
			,ManufactureOrders.StagedDate AS StagedDate
			,StyleColorChoices.ColorComments AS PlayerDetails
			,Styles.Description2 AS Team
			,d2.DropDownValue AS Gender
			,ISNULL(Orders.TotalPrice, 0) As Price
			,StatusNames.StatusName AS Status
			,CASE 
				WHEN UPPER(Orders.Comments6) = 'PROMOTIONAL' THEN 'PRIORITY 1'
				WHEN ISNULL(Orders.Comments24, '') = '' THEN 'PRIORITY 2'
				ELSE Orders.Comments24
			END AS PriorityPPM
	FROM ManufactureOrders
		LEFT OUTER JOIN Warehouses ON ManufactureOrders.WarehouseID = Warehouses.WarehouseID
		LEFT OUTER JOIN Orders ON ManufactureOrders.OrderID = Orders.OrderID
		LEFT OUTER JOIN Addresses ON ManufactureOrders.CustomerID = Addresses.AddressID
		LEFT OUTER JOIN StatusNames ON ManufactureOrders.StatusID = StatusNames.StatusID
		LEFT OUTER JOIN OrderItems ON ManufactureOrders.FirstOrderItemID = OrderItems.OrderItemID
		LEFT OUTER JOIN StyleColors ON OrderItems.StyleColorID = StyleColors.StyleColorID
		LEFT OUTER JOIN Styles ON OrderItems.StyleID = Styles.StyleID
		LEFT OUTER JOIN StyleCategories ON Styles.StyleCategoryID = StyleCategories.StyleCategoryID
		LEFT OUTER JOIN Divisions ON Styles.DivisionID = Divisions.DivisionID
		LEFT OUTER JOIN DropDownValues2 ON Orders.OrderTypeID3 = DropDownValues2.DropDownValueID
		LEFT OUTER JOIN EnumValues with (NOLOCK) ON ManufactureOrders.MfgOrderTypeID = EnumValues.EnumValueID
		LEFT OUTER JOIN Seasons with (NOLOCK) ON Styles.SeasonID = Seasons.SeasonID
		LEFT OUTER JOIN StyleColorChoices with (NOLOCK) ON Styles.StyleID = StyleColorChoices.StyleID AND StyleColors.StyleColorID = StyleColorChoices.StyleColorID
		LEFT OUTER JOIN DropDownValues2 as d2 with (NOLOCK) ON Styles.ProductClassID = d2.DropDownValueID
		LEFT JOIN (SELECT 	Shipments.OrderID	,MAX(CONVERT(DATE,Shipments.ShipDate)) As ShipDate	FROM Shipments	GROUP BY Shipments.OrderID) AS Shipments2	ON Orders.OrderID = Shipments2.OrderID
	WHERE ManufactureOrders.StatusID NOT IN (95,20)
		AND ManufactureOrders.MfgOrderTypeID IN (SELECT EnumValueID FROM EnumValues WHERE EnumName='MfgOrderType' AND EnumValue In('New','Standard'))
		AND ( Orders.RequiredDate  > { d '2024-01-01' } OR ManufactureOrders.TargetDate > { d '2024-01-01' })
		AND ManufactureOrders.Archived = 0
		AND Warehouses.WarehouseName = 'FGW-Arena-Modified'
	UNION
	SELECT --top 20

			Orders.OrderID
			,CASE WHEN ManufactureOrders.CutNumber IS NULL THEN ManufactureOrders.ManufactureNumber + '-001' ELSE ManufactureOrders.ManufactureNumber + '-' + ManufactureOrders.CutNumber END AS KEY1
			,CASE WHEN (Addresses.CompanyNumber = 'NHL' OR Divisions.DivisionName LIKE '%Nike%' ) and not Addresses.CompanyNumber LIKE '%Fanatics%' 
			THEN 'Nike' WHEN Divisions.DivisionName LIKE '%Fanatics%' 
			THEN 'Fanatics'  
			ELSE Addresses.CompanyNumber END AS Customer
		   ,RTrim(Case When ManufactureOrders.ManufactureID<=999999 Then 
							Replicate('0', 6-Len(Cast(ManufactureOrders.ManufactureID As Nchar))) 
						Else '' End
					+ Cast(ManufactureOrders.ManufactureID As Nchar)) As SerialNumber
			,ManufactureOrders.QuantityOrdered AS QuantityOrdered
			,Warehouses.WarehouseName AS GoodsWarehouse
			,DropDownValues2.DropDownValue
			,EnumValues.EnumValue
			,Divisions.DivisionName
			,StyleCategories.StyleCategoryName
			,StyleColors.StyleColorDescription
			,'SO-Full Order ' AS TypeOrder
			,Styles.StyleNumber AS Style
			,Seasons.SeasonCode
			,ManufactureOrders.ManufactureNumber AS MO
			,Orders.PONumber As PONumber
			,ISNULL(ManufactureOrders.CutNumber, '001') AS Cut
			,CONVERT(DATE, Orders.OriginalRequiredDate) As OGAC
			,CONVERT(DATE, ManufactureOrders.TargetDate) AS XFactoryDate
			,CONVERT(DATE, Orders.RequiredDate) AS GACDate
			,CONVERT(DATE,Shipments2.ShipDate) As ShipDate
			,ISNULL(Orders.ShipCount, 0) AS ShipCount
			,CONVERT(DATE, ManufactureOrders.WithdrawDateB1) As PlanMaterialDate
			,CONVERT(DATE, ManufactureOrders.MaterialDate) AS CurrentPRDFabric
			,CONVERT(DATE, ManufactureOrders.TrimMaterialDate)  AS CurrentPRDTrim
			,CONVERT(DATE, ManufactureOrders.SuppliesMaterialDate) AS CurrentPRDSupplies
			,Orders.Comments2 AS BuyMonthYear
			,Styles.StyleName AS StyleName
			,Styles.Comments3 AS StyleComments3
			,StyleColors.StyleColorName AS Color
			,Orders.Comments6 AS OrderComments6
			,Orders.Comments3
			,ISNULL(Orders.RequestCount, 0) AS CustomerQty
			,ManufactureOrders.OrderDate AS MarkerDate
			,ManufactureOrders.StagedDate AS StagedDate
			,StyleColorChoices.ColorComments AS PlayerDetails
			,Styles.Description2 AS Team
			,d2.DropDownValue AS Gender
			,ISNULL(Orders.TotalPrice, 0) As Price
			,StatusNames.StatusName AS Status
			,CASE 
				WHEN UPPER(Orders.Comments6) = 'PROMOTIONAL' THEN 'PRIORITY 1'
				WHEN ISNULL(Orders.Comments24, '') = '' THEN 'PRIORITY 2'
				ELSE Orders.Comments24
			END AS Priority
	FROM ManufactureOrders
		LEFT OUTER JOIN Warehouses ON ManufactureOrders.WarehouseID = Warehouses.WarehouseID
		LEFT OUTER JOIN Orders ON ManufactureOrders.OrderID = Orders.OrderID
		LEFT OUTER JOIN Addresses ON ManufactureOrders.CustomerID = Addresses.AddressID
		LEFT OUTER JOIN StatusNames ON ManufactureOrders.StatusID = StatusNames.StatusID
		LEFT OUTER JOIN OrderItems ON ManufactureOrders.FirstOrderItemID = OrderItems.OrderItemID
		LEFT OUTER JOIN StyleColors ON OrderItems.StyleColorID = StyleColors.StyleColorID
		LEFT OUTER JOIN Styles ON OrderItems.StyleID = Styles.StyleID
		LEFT OUTER JOIN StyleCategories ON Styles.StyleCategoryID = StyleCategories.StyleCategoryID
		LEFT OUTER JOIN Divisions ON Styles.DivisionID = Divisions.DivisionID
		LEFT OUTER JOIN DropDownValues2 ON Orders.OrderTypeID3 = DropDownValues2.DropDownValueID
		LEFT OUTER JOIN EnumValues with (NOLOCK) ON ManufactureOrders.MfgOrderTypeID = EnumValues.EnumValueID
		LEFT OUTER JOIN Seasons with (NOLOCK) ON Styles.SeasonID = Seasons.SeasonID
		LEFT OUTER JOIN StyleColorChoices with (NOLOCK) ON Styles.StyleID = StyleColorChoices.StyleID AND StyleColors.StyleColorID = StyleColorChoices.StyleColorID
		LEFT OUTER JOIN DropDownValues2 as d2 with (NOLOCK) ON Styles.ProductClassID = d2.DropDownValueID
		LEFT JOIN (SELECT 	Shipments.OrderID	,MAX(CONVERT(DATE,Shipments.ShipDate)) As ShipDate	FROM Shipments	GROUP BY Shipments.OrderID) AS Shipments2	ON Orders.OrderID = Shipments2.OrderID
	WHERE ManufactureOrders.StatusID NOT IN (95,20) 
		--and StatusNames.StatusName != 'Hold'
		AND ManufactureOrders.MfgOrderTypeID IN (SELECT EnumValueID FROM EnumValues WHERE EnumName='MfgOrderType' AND EnumValue In('New','Cut', 'Standard'))
		AND (CONVERT(DATE, Orders.RequiredDate) > '2024-01-01' OR CONVERT(DATE, ManufactureOrders.TargetDate) > '2024-01-01')
		AND ManufactureOrders.Archived = 0
		--AND (StyleCategories.StyleCategoryName <> 'Sub Assembly' OR Addresses.CompanyNumber NOT IN ('NHL', 'IKC'))
		AND (StyleCategories.StyleCategoryName <> 'Sub Assembly')
		AND Addresses.CompanyNumber NOT IN ('IKC')
		AND (DropDownValues2.DropDownValue NOT LIKE 'SO-Packing Order' or DropDownValues2.DropDownValue is null)
		AND     Warehouses.WarehouseName in
		('FGW-27 Calle',
		'FGW-Southern',
		'FGW-Arena',
		'FGW-Arena-Stock',
		'FGW-Arena-RQT',
		'FGW-Arena-Desarrollo'
		)
	UNION
	SELECT --top 20
			Orders.OrderID
			,CASE WHEN ManufactureOrders.CutNumber IS NULL THEN ManufactureOrders.ManufactureNumber + '-001' ELSE ManufactureOrders.ManufactureNumber + '-' + ManufactureOrders.CutNumber END AS KEY1
			,CASE WHEN Addresses.CompanyNumber = 'NHL' OR Divisions.DivisionName LIKE '%Nike%' THEN 'Nike' WHEN Divisions.DivisionName LIKE '%Fanatics%' THEN 'Fanatics'  ELSE Addresses.CompanyNumber END AS Customer
		   ,RTrim(Case When ManufactureOrders.ManufactureID<=999999 Then 
							Replicate('0', 6-Len(Cast(ManufactureOrders.ManufactureID As Nchar))) 
						Else '' End
					+ Cast(ManufactureOrders.ManufactureID As Nchar)) As SerialNumber
			,ManufactureOrders.QuantityOrdered AS QuantityOrdered
			,Warehouses.WarehouseName AS GoodsWarehouse
			,DropDownValues2.DropDownValue
			,EnumValues.EnumValue
			,Divisions.DivisionName
			,StyleCategories.StyleCategoryName
			,StyleColors.StyleColorDescription
			,'SO-Full Order ' AS TypeOrder
			,Styles.StyleNumber AS Style
			,Seasons.SeasonCode
			,ManufactureOrders.ManufactureNumber AS MO
			,Orders.PONumber As PONumber
			,ManufactureOrders.CutNumber AS Cut
			,CONVERT(DATE, Orders.OriginalRequiredDate) As OGAC
			,CONVERT(DATE, ManufactureOrders.TargetDate) AS XFactoryDate
			,CONVERT(DATE, Orders.RequiredDate) AS GACDate
			,CONVERT(DATE,Shipments2.ShipDate) As ShipDate
			,ISNULL(Orders.ShipCount, 0) AS ShipCount
			,CONVERT(DATE, ManufactureOrders.WithdrawDateB1) As PlanMaterialDate
			,CONVERT(DATE, ManufactureOrders.MaterialDate) AS CurrentPRDFabric
			,CONVERT(DATE, ManufactureOrders.TrimMaterialDate)  AS CurrentPRDTrim
			,CONVERT(DATE, ManufactureOrders.SuppliesMaterialDate) AS CurrentPRDSupplies
			,Orders.Comments2 AS BuyMonthYear
			,Styles.StyleName AS StyleName
			,Styles.Comments3 AS StyleComments3
			,StyleColors.StyleColorName AS Color
			,Orders.Comments6 AS OrderComments6
			,Orders.Comments3
			,ISNULL(Orders.RequestCount, 0) AS CustomerQty
			,ManufactureOrders.OrderDate AS MarkerDate
			,ManufactureOrders.StagedDate AS StagedDate
			,StyleColorChoices.ColorComments AS PlayerDetails
			,Styles.Description2 AS Team
			,d2.DropDownValue AS Gender
			,ISNULL(Orders.TotalPrice, 0) As Price
			,StatusNames.StatusName AS Status
			,CASE 
				WHEN UPPER(Orders.Comments6) = 'PROMOTIONAL' THEN 'PRIORITY 1'
				WHEN ISNULL(Orders.Comments24, '') = '' THEN 'PRIORITY 2'
				ELSE Orders.Comments24
			END AS Priority
	FROM ManufactureOrders
		LEFT OUTER JOIN Warehouses ON ManufactureOrders.WarehouseID = Warehouses.WarehouseID
		LEFT OUTER JOIN Orders ON ManufactureOrders.OrderID = Orders.OrderID
		LEFT OUTER JOIN Addresses ON ManufactureOrders.CustomerID = Addresses.AddressID
		LEFT OUTER JOIN StatusNames ON ManufactureOrders.StatusID = StatusNames.StatusID
		LEFT OUTER JOIN OrderItems ON ManufactureOrders.FirstOrderItemID = OrderItems.OrderItemID
		LEFT OUTER JOIN StyleColors ON OrderItems.StyleColorID = StyleColors.StyleColorID
		LEFT OUTER JOIN Styles ON OrderItems.StyleID = Styles.StyleID
		LEFT OUTER JOIN StyleCategories ON Styles.StyleCategoryID = StyleCategories.StyleCategoryID
		LEFT OUTER JOIN Divisions ON Styles.DivisionID = Divisions.DivisionID
		LEFT OUTER JOIN DropDownValues2 ON Orders.OrderTypeID3 = DropDownValues2.DropDownValueID
		LEFT OUTER JOIN EnumValues with (NOLOCK) ON ManufactureOrders.MfgOrderTypeID = EnumValues.EnumValueID
		LEFT OUTER JOIN Seasons with (NOLOCK) ON Styles.SeasonID = Seasons.SeasonID
		LEFT OUTER JOIN StyleColorChoices with (NOLOCK) ON Styles.StyleID = StyleColorChoices.StyleID AND StyleColors.StyleColorID = StyleColorChoices.StyleColorID
		LEFT OUTER JOIN DropDownValues2 as d2 with (NOLOCK) ON Styles.ProductClassID = d2.DropDownValueID
		LEFT JOIN (SELECT 	Shipments.OrderID	,MAX(CONVERT(DATE,Shipments.ShipDate)) As ShipDate	FROM Shipments	GROUP BY Shipments.OrderID) AS Shipments2	ON Orders.OrderID = Shipments2.OrderID
	WHERE ManufactureOrders.StatusID NOT IN (95,20) 
		AND ManufactureOrders.MfgOrderTypeID IN (SELECT EnumValueID FROM EnumValues WHERE EnumName='MfgOrderType' AND EnumValue In('New','Cut', 'Standard'))
		AND StyleCategories.StyleCategoryName = 'Jersey'
		AND ( Case When Exists((
		Select * From ManufactureDetails
		Where ManufactureID=ManufactureOrders.ManufactureID And RawMaterialID Is Not Null
		)) Then 1 Else 0 End ) = 1
	UNION
	SELECT --top 20
			Orders.OrderID
			,CASE WHEN ManufactureOrders.CutNumber IS NULL THEN ManufactureOrders.ManufactureNumber + '-001' ELSE ManufactureOrders.ManufactureNumber + '-' + ManufactureOrders.CutNumber END AS KEY1
			,CASE WHEN Addresses.CompanyNumber = 'NHL' OR Divisions.DivisionName LIKE '%Nike%' THEN 'Nike' WHEN Divisions.DivisionName LIKE '%Fanatics%' THEN 'Fanatics'  ELSE Addresses.CompanyNumber END AS Customer
		   ,RTrim(Case When ManufactureOrders.ManufactureID<=999999 Then 
							Replicate('0', 6-Len(Cast(ManufactureOrders.ManufactureID As Nchar))) 
						Else '' End
					+ Cast(ManufactureOrders.ManufactureID As Nchar)) As SerialNumber
			,ManufactureOrders.QuantityOrdered AS QuantityOrdered
			,Warehouses.WarehouseName AS GoodsWarehouse
			,DropDownValues2.DropDownValue
			,EnumValues.EnumValue
			,Divisions.DivisionName
			,StyleCategories.StyleCategoryName
			,StyleColors.StyleColorDescription
			,'SO-Full Order ' AS TypeOrder
			,Styles.StyleNumber AS Style
			,Seasons.SeasonCode
			,ManufactureOrders.ManufactureNumber AS MO
			,Orders.PONumber As PONumber
			,ISNULL(ManufactureOrders.CutNumber, '001') AS Cut
			,CONVERT(DATE, Orders.OriginalRequiredDate) As OGAC
			,CONVERT(DATE, ManufactureOrders.TargetDate) AS XFactoryDate
			,CONVERT(DATE, Orders.RequiredDate) AS GACDate
			,CONVERT(DATE,Shipments2.ShipDate) As ShipDate
			,ISNULL(Orders.ShipCount, 0) AS ShipCount
			,CONVERT(DATE, ManufactureOrders.WithdrawDateB1) As PlanMaterialDate
			,CONVERT(DATE, ManufactureOrders.MaterialDate) AS CurrentPRDFabric
			,CONVERT(DATE, ManufactureOrders.TrimMaterialDate)  AS CurrentPRDTrim
			,CONVERT(DATE, ManufactureOrders.SuppliesMaterialDate) AS CurrentPRDSupplies
			,Orders.Comments2 AS BuyMonthYear
			,Styles.StyleName AS StyleName
			,Styles.Comments3 AS StyleComments3
			,StyleColors.StyleColorName AS Color
			,Orders.Comments6 AS OrderComments6
			,Orders.Comments3
			,ISNULL(Orders.RequestCount, 0) AS CustomerQty
			,ManufactureOrders.OrderDate AS MarkerDate
			,ManufactureOrders.StagedDate AS StagedDate
			,StyleColorChoices.ColorComments AS PlayerDetails
			,Styles.Description2 AS Team
			,d2.DropDownValue AS Gender
			,ISNULL(Orders.TotalPrice, 0) As Price
			,StatusNames.StatusName AS Status
			,CASE 
				WHEN UPPER(Orders.Comments6) = 'PROMOTIONAL' THEN 'PRIORITY 1'
				WHEN ISNULL(Orders.Comments24, '') = '' THEN 'PRIORITY 2'
				ELSE Orders.Comments24
			END AS Priority
	FROM ManufactureOrders
		LEFT OUTER JOIN Warehouses ON ManufactureOrders.WarehouseID = Warehouses.WarehouseID
		LEFT OUTER JOIN Orders ON ManufactureOrders.OrderID = Orders.OrderID
		LEFT OUTER JOIN Addresses ON ManufactureOrders.CustomerID = Addresses.AddressID
		LEFT OUTER JOIN StatusNames ON ManufactureOrders.StatusID = StatusNames.StatusID
		LEFT OUTER JOIN OrderItems ON ManufactureOrders.FirstOrderItemID = OrderItems.OrderItemID
		LEFT OUTER JOIN StyleColors ON OrderItems.StyleColorID = StyleColors.StyleColorID
		LEFT OUTER JOIN Styles ON OrderItems.StyleID = Styles.StyleID
		LEFT OUTER JOIN StyleCategories ON Styles.StyleCategoryID = StyleCategories.StyleCategoryID
		LEFT OUTER JOIN Divisions ON Styles.DivisionID = Divisions.DivisionID
		LEFT OUTER JOIN DropDownValues2 ON Orders.OrderTypeID3 = DropDownValues2.DropDownValueID
		LEFT OUTER JOIN EnumValues with (NOLOCK) ON ManufactureOrders.MfgOrderTypeID = EnumValues.EnumValueID
		LEFT OUTER JOIN Seasons with (NOLOCK) ON Styles.SeasonID = Seasons.SeasonID
		LEFT OUTER JOIN StyleColorChoices with (NOLOCK) ON Styles.StyleID = StyleColorChoices.StyleID AND StyleColors.StyleColorID = StyleColorChoices.StyleColorID
		LEFT OUTER JOIN DropDownValues2 as d2 with (NOLOCK) ON Styles.ProductClassID = d2.DropDownValueID
		LEFT JOIN (SELECT 	Shipments.OrderID	,MAX(CONVERT(DATE,Shipments.ShipDate)) As ShipDate	FROM Shipments	GROUP BY Shipments.OrderID) AS Shipments2	ON Orders.OrderID = Shipments2.OrderID
	WHERE ManufactureOrders.StatusID NOT IN (95,20) 
		AND ManufactureOrders.MfgOrderTypeID IN (SELECT EnumValueID FROM EnumValues WHERE EnumName='MfgOrderType' AND EnumValue In('New','Standard'))
		AND StyleCategories.StyleCategoryName = 'Sub Assembly'
		AND ( Case When Exists((
		Select * From ManufactureDetails
		Where ManufactureID=ManufactureOrders.ManufactureID And RawMaterialID Is Not Null
		)) Then 1 Else 0 End ) = 1
		AND ( Orders.RequiredDate  > { d '2024-01-01' } OR ManufactureOrders.TargetDate > { d '2024-01-01' })

) AS B1
--WHERE B1.PONumber = '430067' 
--WHERE B1.MO = '2016034'
WHERE B1.[SerialNumber] = 2943851
"""

@router.get("/test")
async def get_promised(db: Session = Depends(get_db)):
    connection = conn()  # ✅ no sobreescribas db
    result = await connection.connect_and_execute(SupplyNew, textsql)  # ✅ usa otro nombre
    df_sql = pd.DataFrame(result)
    df_pd = df_sql[['MO','CuttingNum','SubProcess']]

    result = await connection.connect_and_execute(polypm, textsql2)
    df_sql2 = pd.DataFrame(result)
    print(df_sql2)

    items = db.query(PromisedDateSerialStatus).filter(PromisedDateSerialStatus.Status_flag == 1).all()
    df_orm = pd.DataFrame([item.__dict__ for item in items])
    print(df_orm)

    #d1=pd.merge(df_sql,df_orm on=[])
    return True 
    


