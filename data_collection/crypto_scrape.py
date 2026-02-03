import requests
import pandas as pd
from datetime import datetime, timedelta
from google.cloud import bigquery
from dotenv import load_dotenv
import os

load_dotenv()  # load .env first

# -----------------------------
# ENV / BigQuery setup
# -----------------------------
project_id = os.getenv('PROJECT_ID')
dataset_id = os.getenv('DATASET_ID2')
cred_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = cred_path

print("Using credentials:", cred_path)
print("File exists?", os.path.isfile(cred_path))

client = bigquery.Client(project=os.getenv("PROJECT_ID"))

# -----------------------------
# CONFIG
# -----------------------------
ASSET = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT"]
INTERVAL = "1h"  # hourly candlesticks
BASE_URL = "https://api.binance.com/api/v3/klines"

# -----------------------------
# FETCH DATA
# -----------------------------
def fetch_last_24h(symbol):
    """Fetch hourly candlesticks for the past 24 hours"""
    end_time = int(datetime.utcnow().timestamp() * 1000)  # now in ms
    start_time = int((datetime.utcnow() - timedelta(hours=24)).timestamp() * 1000)  # 24h ago in ms

    params = {
        "symbol": symbol,
        "interval": INTERVAL,
        "startTime": start_time,
        "endTime": end_time,
        "limit": 1000
    }

    response = requests.get(BASE_URL, params=params)
    response.raise_for_status()
    klines = response.json()

    rows = []
    for k in klines:
        rows.append({
            "symbol": symbol,
            "open_time": datetime.fromtimestamp(k[0]/1000),
            "open": float(k[1]),
            "high": float(k[2]),
            "low": float(k[3]),
            "close": float(k[4]),
            "volume": float(k[5]),
            "number_of_trades": k[8]
        })
    return pd.DataFrame(rows)

# -----------------------------
# UPLOAD TO BIGQUERY
# -----------------------------
def upload_to_bigquery(df):
    table_id = f"{project_id}.{dataset_id}.raw_crypto"
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",  # replace table
        autodetect=True
    )

    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()  # wait for the job to finish
    print(f"Uploaded {len(df)} rows to {table_id}")

# -----------------------------
# MAIN
# -----------------------------
if __name__ == "__main__":
    all_rows = []
    for asset in ASSET:
        df_asset = fetch_last_24h(asset)
        all_rows.append(df_asset)

    df = pd.concat(all_rows, ignore_index=True)
    print(df)
    upload_to_bigquery(df)

    

