from sqlalchemy import create_engine, Table, Column, Integer, String, MetaData

# Initialize SQLite engine and metadata
engine = create_engine("sqlite:///private_funds.db")
metadata = MetaData()

# Define the table schema
private_fund_data = Table(
    "private_fund_data", metadata,
    Column("FirmCrdNb", Integer),
    Column("Address", String),
    Column("Phone Number", String),
    Column("Compensation Arrangements", String),
    Column("Number of employees performing investment advisory functions", String),
    Column("Type of Client and Amount of Regulatory Assets Under Management", String),
    Column("Private Fund Name", String),
    Column("Private Fund ID", String),
    Column("Gross Asset Value", Integer),
    Column("Signatory", String),
    Column("SECRgnCD", String),
    Column("SECNb", String),
    Column("BusNm", String),
    Column("LegalNm", String),
    Column("UmbrRgstn", String),
    Column("DownloadPath", String)
)

# Create the table in the database
metadata.create_all(engine)
