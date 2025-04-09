import pytest
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from fastapi.testclient import TestClient
from funds_api import app

client = TestClient(app)

# ---------------------- GET /funds/{firm_crd} ----------------------

def test_get_funds_valid_firm():
    response = client.get("/funds/160882")
    assert response.status_code == 200
    assert isinstance(response.json(), list)
    assert "Private_Fund_ID" in response.json()[0]

def test_get_funds_invalid_firm():
    response = client.get("/funds/999999")  # assuming it doesn't exist
    assert response.status_code == 404

# ---------------------- GET /funds/top/{n} ----------------------

def test_get_top_funds_default():
    response = client.get("/funds/top/2")
    assert response.status_code == 200
    assert len(response.json()) == 2

def test_get_top_funds_zero():
    response = client.get("/funds/top/0")
    assert response.status_code == 200
    assert response.json() == []

# ---------------------- POST /funds/add ----------------------

def test_add_new_fund_success():
    payload = {
        "FirmCrdNb": 160882,
        "Private_Fund_ID": "805-TEST12345",
        "Private_Fund_Name": "TEST FUND",
        "Gross_Asset_Value": 1234567
    }
    response = client.post("/funds/add", json=payload)
    assert response.status_code == 200
    assert response.json()["Private_Fund_ID"] == "805-TEST12345"

def test_add_existing_fund():
    payload = {
        "FirmCrdNb": 160882,
        "Private_Fund_ID": "805-13387801972",  # existing fund ID
        "Private_Fund_Name": "DUPLICATE FUND",
        "Gross_Asset_Value": 99999
    }
    response = client.post("/funds/add", json=payload)
    assert response.status_code == 400
