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
def get_latest_timestamp(symbol):
    query = f"""SELECT 
                    MAX(open_time) as max_time
                FROM `{project_id}.{dataset_id}.raw_crypto`
                WHERE symbol = '{symbol}'
"""
    
    result = client.query(query).result()
    
    for row in result:
        return row.max_time
    
def fetch_new_data(symbol):
    last_timestamp = get_latest_timestamp(symbol)

    if last_timestamp:
        start_time = int(last_timestamp.timestamp() * 1000)
    else:
        # First run → pull 24h
        start_time = int((datetime.utcnow() - timedelta(hours=24)).timestamp() * 1000)

    end_time = int(datetime.utcnow().timestamp() * 1000)

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
        write_disposition="WRITE_APPEND",  
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
        df_asset = fetch_new_data(asset)
        if not df_asset.empty:
            all_rows.append(df_asset)

    if all_rows:
        df = pd.concat(all_rows, ignore_index=True)
        upload_to_bigquery(df)
    else:
        print("No new data to upload.")

    

