import requests
import pandas as pd
import sqlite3

print("Starting ETL pipeline...")

# ========== EXTRACT ==========
print("Extracting data from API...")
url = "https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd"

try:
    r = requests.get(url)
    print("Status Code:", r.status_code)
    data = r.json()

    print("Number of records extracted:", len(data))
except Exception as e:
    print("EXTRACT ERROR:", e)
    exit()

# ========== TRANSFORM ==========
print("\nTransforming data...")
try:
    df = pd.DataFrame(data)
    df = df[['id', 'symbol', 'current_price', 'high_24h', 'low_24h']]
    print("DataFrame head:")
    print(df.head())
except Exception as e:
    print("TRANSFORM ERROR:", e)
    exit()

# ========== LOAD ==========
print("\nLoading data into SQL...")
try:
    conn = sqlite3.connect("crypto.db")
    df.to_sql("crypto_prices", conn, if_exists="replace", index=False)
    conn.close()
    print("Database saved as crypto.db")
except Exception as e:
    print("LOAD ERROR:", e)
    exit()

print("\nETL pipeline finished successfully.")
