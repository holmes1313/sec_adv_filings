import os
import re
import io
import gzip
import time
import logging
import requests
import subprocess
import pandas as pd
import xml.etree.ElementTree as ET
from datetime import datetime
from tempfile import NamedTemporaryFile
from math import ceil
from pdfminer.high_level import extract_text
from sqlalchemy import create_engine

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.max_colwidth', None)
#pd.set_option('display.width', None)

# Set the working date
WorkingDate = datetime(2025, 4, 24)
url = f"https://reports.adviserinfo.sec.gov/reports/CompilationReports/IA_FIRM_SEC_Feed_{WorkingDate:%m_%d_%Y}.xml.gz"
response = requests.get(url)

if response.status_code == 200:
    with gzip.GzipFile(fileobj=io.BytesIO(response.content)) as gz:
        xml_content = gz.read()
    decoded_content = xml_content.decode('ISO-8859-1')
    df = pd.read_xml(io.StringIO(decoded_content), xpath='//Info')
else:
    raise Exception(f"Failed to download XML. Status code: {response.status_code}")

# Add download paths
df['DownloadPath'] = df['FirmCrdNb'].apply(lambda x: f"https://reports.adviserinfo.sec.gov/reports/ADV/{x}/PDF/{x}.pdf")

# Filter for specific firms
target_firmCrdNbs = [160882, 160021, 317731]
df = df[df["FirmCrdNb"].isin(target_firmCrdNbs)]

# Download PDFs
save_dir = "source_pdf"
os.makedirs(save_dir, exist_ok=True)
start_time = time.time()

for url in df['DownloadPath']:
    file_name = url.split('/')[-1]
    save_path = os.path.join(save_dir, file_name)

    if os.path.isfile(save_path):
        logging.info(f"File {file_name} already exists. Skipping download.")
        continue

    response = requests.get(url)
    if response.status_code == 200:
        with open(save_path, 'wb') as f:
            f.write(response.content)
        logging.info(f"Downloaded {file_name}")
    else:
        logging.error(f"Failed to download {file_name}. Status code: {response.status_code}")

logging.info(f"Completed the download process in {(time.time() - start_time) / 60:.2f} minutes.")


def extract_private_funds(text):
    section_match = re.search(
        r"SECTION 7\.B\.\(1\) Private Fund Reporting(.*?)SECTION 7\.B\.\(2\) Private Fund Reporting",
        text, re.DOTALL)
    if not section_match:
        return []

    section_text = section_match.group(1)
    pattern = re.compile(
        r"1\.\(a\)Name of the private fund:(.*?)"
        r"\(b\)Private fund identification number:.*?(805-\d+).*?"
        r"11\.Current gross asset value of the private fund:\$ *(\d[\d,]*)",
        re.DOTALL)

    raw_data = []
    for match in pattern.finditer(section_text):
        name = match.group(1).strip().replace("\n", " ")
        fund_id = match.group(2).strip()
        value = int(match.group(3).replace(",", ""))
        raw_data.append([name, fund_id, value])

    latest = {}
    for name, fund_id, value in raw_data:
        key = (name, fund_id)
        if key not in latest or value > latest[key]:
            latest[key] = value

    return [[name, fund_id, value] for (name, fund_id), value in latest.items()]


def parse_pdf(pdf_file, save_dir="source_pdf"):
    pdf_path = os.path.join(save_dir, pdf_file)
    try:
        text = extract_text(pdf_path)
        with open("extracted_text.txt", "w", encoding="utf-8") as f:
            f.write(text)
    except Exception as e:
        print(f"Error extracting text from PDF: {e}")
        return []

    def clean_text(s):
        return ' '.join(s.split()).strip()

    # Extract fields
    address = phone = employees = signatory = None
    addr_pattern = re.compile(r"Principal Office and Place of Business.*?Number and Street 1:?\s*(.*?)\s*Number and Street 2:?\s*(.*?)\s*City:?\s*(.*?)\s*State:?\s*(.*?)\s*Country:?\s*(.*?)\s*ZIP\+4/Postal Code:?\s*(\S+)", re.DOTALL | re.IGNORECASE)
    addr_match = addr_pattern.search(text)
    if addr_match:
        street1, street2, city, state, country, postal = addr_match.groups()
        address = clean_text(f"{street1}, {street2}, {city}, {state}, {country}, {postal}")

    phone_match = re.search(r"Telephone number at this location:?\s*(\+?\d+\s*\(?\d+\)?\s*[\d\s]+)", text, re.IGNORECASE)
    if phone_match:
        phone = clean_text(phone_match.group(1))

    emp_match = re.search(r"5\.B\.\(1\).*?how many.*?investment advisory functions.*?\n?.*?(\d+)", text, re.DOTALL | re.IGNORECASE)
    if emp_match:
        employees = emp_match.group(1)

    sig_match = re.findall(r"Printed Name:\s*([A-Z][A-Za-z\s]+?)\s+Title:", text, re.IGNORECASE)
    if sig_match:
        signatory = sig_match[-1].strip()

    private_funds = extract_private_funds(text)
    results = []
    for fund_name, fund_id, asset_value in private_funds:
        results.append({
            "FirmCrdNb": int(pdf_file.split(".")[0]),
            "Address": address,
            "Phone Number": phone,
            "Compensation Arrangements": None,
            "Number of employees performing investment advisory functions": employees,
            "Type of Client and Amount of Regulatory Assets Under Management": None,
            "Private Fund Name": fund_name,
            "Private Fund ID": fund_id,
            "Gross Asset Value": asset_value,
            "Signatory": signatory
        })
    return results

process_data = False
if process_data:
    # --- Process and Merge ---
    master_df = pd.DataFrame()
    for crd in target_firmCrdNbs:
        pdf_file = f"{crd}.pdf"
        parsed_data = parse_pdf(pdf_file)
        df_parsed = pd.DataFrame(parsed_data)
        master_df = pd.concat([master_df, df_parsed], ignore_index=True)

    merged_df = pd.merge(master_df, df, on="FirmCrdNb", how="left")
    print(merged_df.shape)
    merged_df.to_csv('merged_data.csv', index=False)
    print("Merged DataFrame written to 'merged_data.csv'")

    # --- Save to SQLite ---
    engine = create_engine("sqlite:///private_funds.db")
    merged_df.to_sql("private_fund_data", engine, if_exists="append", index=False)

    with engine.connect() as conn:
        result = pd.read_sql("SELECT * FROM private_fund_data", conn)
        print(result.head())

print("data ingestion completed")
