from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field, validator
from typing import List
import sqlite3

app = FastAPI()
DB_PATH = "private_funds.db"

# Data Models
class PrivateFund(BaseModel):
    Private_Fund_ID: str
    Private_Fund_Name: str
    FirmCrdNb: int
    Gross_Asset_Value: int

    @validator('Gross_Asset_Value')
    def validate_value(cls, value):
        if value < 0:
            raise ValueError("Gross Asset Value must be non-negative")
        return value


class NewFundRequest(BaseModel):
    FirmCrdNb: int
    Private_Fund_ID: str
    Private_Fund_Name: str
    Gross_Asset_Value: int

    @validator('Gross_Asset_Value')
    def validate_value(cls, value):
        if value < 0:
            raise ValueError("Gross Asset Value must be non-negative")
        return value


def query_db(query: str, params: tuple = ()):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(query, params)
    rows = cur.fetchall()
    conn.commit()
    conn.close()
    return rows

# Endpoints
@app.get("/funds/{firm_crd}", response_model=List[PrivateFund])
def get_funds_by_firm(firm_crd: int):
    rows = query_db(
        "SELECT `Private Fund ID`, `Private Fund Name`, FirmCrdNb, `Gross Asset Value` FROM private_fund_data WHERE FirmCrdNb = ?",
        (firm_crd,)
    )
    if not rows:
        raise HTTPException(status_code=404, detail="Firm not found or has no funds.")
    return [PrivateFund(
        Private_Fund_ID=r[0],
        Private_Fund_Name=r[1],
        FirmCrdNb=r[2],
        Gross_Asset_Value=r[3]
    ) for r in rows]


@app.get("/funds/top/{n}", response_model=List[PrivateFund])
def get_top_funds(n: int = 5):
    rows = query_db(
        "SELECT `Private Fund ID`, `Private Fund Name`, FirmCrdNb, `Gross Asset Value` FROM private_fund_data ORDER BY `Gross Asset Value` DESC LIMIT ?",
        (n,)
    )
    return [PrivateFund(
        Private_Fund_ID=r[0],
        Private_Fund_Name=r[1],
        FirmCrdNb=r[2],
        Gross_Asset_Value=r[3]
    ) for r in rows]


@app.post("/funds/add", response_model=PrivateFund)
def add_fund(fund: NewFundRequest):
    existing = query_db("SELECT 1 FROM private_fund_data WHERE `Private Fund ID` = ?", (fund.Private_Fund_ID,))
    if existing:
        raise HTTPException(status_code=400, detail="Fund ID already exists.")

    query_db(
        """
        INSERT INTO private_fund_data (`FirmCrdNb`, `Private Fund ID`, `Private Fund Name`, `Gross Asset Value`)
        VALUES (?, ?, ?, ?)
        """,
        (fund.FirmCrdNb, fund.Private_Fund_ID, fund.Private_Fund_Name, fund.Gross_Asset_Value)
    )

    return PrivateFund(
        Private_Fund_ID=fund.Private_Fund_ID,
        Private_Fund_Name=fund.Private_Fund_Name,
        FirmCrdNb=fund.FirmCrdNb,
        Gross_Asset_Value=fund.Gross_Asset_Value
    )


# To run the api: uvicorn funds_api:app --reload