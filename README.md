# SEC ADV Filings Parser & Analyzer

This repository houses a Python-based project to **parse, transform, analyze**, and **expose data from SEC ADV filings**, particularly focusing on private fund information.

---

## Project Structure

| File | Description |
|------|-------------|
| `parse_adv_files.py` | ✅ Step 1: Download Metadata and PDFs<br>✅ Step 2: Extract and Store Information from ADV filings |
| `analyze_funds.py`   | ✅ Step 3: Data Transformation and Analysis<br>✅ Step 4: Generate Excel File<br>✅ Step 8: Identify Top Performing Funds<br>✅ Step 9: Data Visualization |
| `discussion.md`      | ✅ Step 5: Scalability and Performance (Discussion)<br>✅ Step 7: Automated Testing and Data Quality (Discussion) |
| `funds_api.py`       | ✅ Step 6: Integration with External Systems via FastAPI |

## TODO

- The parser took the most time and **is still not 100% complete**.
- Need to **add logic to parse multiple choice selections and tables** from PDF files (e.g., client types, asset breakdown).
- Improve **scalability and performance** to handle large datasets (multi-firm, multi-file environments).
- Expand automated testing coverage and CI integration.


## Notes

- All PDFs and data are sourced from public SEC ADV filings via [adviserinfo.sec.gov](https://adviserinfo.sec.gov/).
- Built using Python, Pandas, SQLAlchemy, Matplotlib, and FastAPI.

## Setup Instructions

```bash
# Install dependencies
pip install -r requirements.txt

# Run the data pipeline
python parse_adv_files.py

# Analyze and visualize results
python analyze_funds.py

# Start the API
uvicorn funds_api:app --reload
