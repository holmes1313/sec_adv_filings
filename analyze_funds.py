import os
import pandas as pd
import sqlite3
import matplotlib.pyplot as plt

# --- Load Data from SQLite ---
conn = sqlite3.connect("private_funds.db")
df = pd.read_sql("SELECT distinct * FROM private_fund_data", conn)
conn.close()

# --- Clean & Transform Data ---
df['Gross Asset Value'] = pd.to_numeric(df['Gross Asset Value'], errors='coerce')
df['FirmCrdNb'] = df['FirmCrdNb'].astype(str).str.strip()
df['Private Fund Name'] = df['Private Fund Name'].str.strip()
df.dropna(subset=['Gross Asset Value', 'Private Fund Name'], inplace=True)

# --- Top-Performing Funds ---
top_funds = df.sort_values(by='Gross Asset Value', ascending=False).head(5)
print("Top 5 Private Funds by Gross Asset Value:")
print(top_funds[['Private Fund Name', 'Gross Asset Value']])

# --- Plot: Top 5 Private Funds by Gross Asset Value ---
os.makedirs("visualization", exist_ok=True)
plt.figure(figsize=(10, 6))
plt.barh(top_funds["Private Fund Name"], top_funds["Gross Asset Value"])
plt.xlabel("Gross Asset Value")
plt.title("Top 5 Private Funds by Gross Asset Value")
plt.gca().invert_yaxis()
plt.tight_layout()
plt.savefig("visualization/top_5_funds.png")
plt.show()

# --- Average Fund Value by Firm ---
avg_per_firm = df.groupby('FirmCrdNb')['Gross Asset Value'].mean().reset_index()
avg_per_firm.rename(columns={'Gross Asset Value': 'Avg Gross Asset Value'}, inplace=True)
print("\nAverage Fund Value per Firm:")
print(avg_per_firm)

# --- Join Top Funds with Firm Averages ---
result = pd.merge(top_funds, avg_per_firm, on='FirmCrdNb', how='left')
print("\nTop Funds with Firm Averages:")
print(result[['Private Fund Name', 'Gross Asset Value', 'Avg Gross Asset Value']])

# --- Optional: Export to Excel ---
output_excel = False
if output_excel:
    df.to_excel("adv_filing.xlsx", index=False)
    result.to_excel("top_funds_analysis.xlsx", index=False)
