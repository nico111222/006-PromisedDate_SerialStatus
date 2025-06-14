from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base
from urllib.parse import quote_plus

# Cadena de conexión ODBC
SupplyNew = "Driver=SQL Server;Server=localhost\\SQLEXPRESS;Database=TEST;UID=sa;PWD=Pass1234;"
polypm = "Driver=SQL Server;Server=10.28.1.8\\POLYSQL2019;Database=Tegra_Production;UID=svc_polyreporting;PWD=P0lyPM2021!!;"

# Codificar la cadena para SQLAlchemy
DATABASE_URL_SupplyNew = "mssql+pyodbc:///?odbc_connect=" + quote_plus(SupplyNew)
DATABASE_URL_polypm = "mssql+pyodbc:///?odbc_connect=" + quote_plus(polypm)

# Crear el motor y la sesión para SupplyNew
engine_supply_new = create_engine(DATABASE_URL_SupplyNew)
SessionLocal_supply_new = sessionmaker(autocommit=False, autoflush=False, bind=engine_supply_new)

# Crear el motor y la sesión para polypm
engine_polypm = create_engine(DATABASE_URL_polypm)
SessionLocal_polypm = sessionmaker(autocommit=False, autoflush=False, bind=engine_polypm)

# Base para los modelos
Base = declarative_base()

def get_db(db_type="SupplyNew"):
    """Devuelve una sesión de la base de datos indicada"""
    if db_type == "SupplyNew":
        db = SessionLocal_supply_new()
    elif db_type == "polypm":
        db = SessionLocal_polypm()
    else:
        raise ValueError("Tipo de base de datos no válido.")
    try:
        yield db
    finally:
        db.close()

# Función de prueba para verificar la conexión
def test_connection(db_type="SupplyNew"):
    """Prueba la conexión a la base de datos indicada"""
    try:
        # Obtener la sesión de la base de datos
        db = next(get_db(db_type))
        # Ejecutar una consulta simple para probar la conexión
        result = db.execute("SELECT 1")
        print(f"✅ Conexión exitosa a {db_type}: {result.scalar()}")
    except Exception as e:
        print(f"❌ Error al conectar a {db_type}: {e}")

# Prueba de conexión a ambas bases de datos
test_connection("SupplyNew")
test_connection("polypm")