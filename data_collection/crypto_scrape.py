import requests
import pandas as pd
from datetime import datetime
import os

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
        

if __name__ == "__main__":
    rows = []

    for asset in ASSET:
        row = fetch_last_hour(asset)
        rows.append(row)
    
    df = pd.DataFrame(rows)
    print(df)

