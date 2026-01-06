import requests
import pandas as pd
from datetime import datetime
from google.cloud import bigquery
from google.oauth2 import service_account
from dotenv import load_dotenv
import os

load_dotenv()  # load .env first

# immediately overwrite environment variable
project_id = os.getenv('PROJECT_ID')
dataset_id = os.getenv('DATASET_ID2')
cred_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = cred_path

print("Using credentials:", cred_path)
print("File exists?", os.path.isfile(cred_path))

from google.cloud import bigquery  # import after setting env
client = bigquery.Client(project=os.getenv("PROJECT_ID"))

# -----------------------------
# CONFIG
# -----------------------------
ASSET = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT"]
INTERVAL = "1h"
LIMIT = 1  


BASE_URL = "https://api.binance.com/api/v3/klines"

# -----------------------------
# FETCH DATA
# -----------------------------
def fetch_last_hour(symbol):
    params = {
        "symbol": symbol,
        "interval": INTERVAL,
        "limit": LIMIT
    }

    response = requests.get(BASE_URL, params=params)
    response.raise_for_status()
    k = response.json()[0]

    return  {
            "symbol": symbol,
            "open_time": datetime.fromtimestamp(k[0] / 1000),
            "open": float(k[1]),
            "high": float(k[2]),
            "low": float(k[3]),
            "close": float(k[4]),
            "volume": float(k[5]),
            "number_of_trades": k[8]
        }
        

def upload_to_bigquery(df):
    table_id = f"{project_id}.{dataset_id}.raw_crypto"
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
        autodetect=True
    )

    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()  # wait for the job to finish
    print(f"Uploaded {len(df)} rows to {table_id}")


if __name__ == "__main__":
    rows = []
    for asset in ASSET:
        rows.append(fetch_last_hour(asset))

    df = pd.DataFrame(rows)
    print(df) 
    upload_to_bigquery(df)
    

