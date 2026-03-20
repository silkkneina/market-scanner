import os
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

# --- DB config ---
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "1433")
DB_NAME = os.getenv("DB_NAME", "MarketData")
DB_USER = os.getenv("DB_USER", "sa")
DB_PASSWORD = os.getenv("DB_PASSWORD")

if not DB_PASSWORD:
    raise ValueError("DB_PASSWORD missing. Put it inside your .env file.")

engine = create_engine(
    f"mssql+pymssql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

# --- Load CSV ---
csv_path = Path(__file__).resolve().parent.parent / "data" / "sp500_constituents.csv"
df = pd.read_csv(csv_path)

df = df.rename(
    columns={
        "Symbol": "wiki_symbol",
        "Security": "security",
        "stooq_symbol": "stooq_symbol",
    }
)

# --- Clear table (safe for now) ---
with engine.begin() as conn:
    conn.execute(text("TRUNCATE TABLE dbo.tickers"))

# --- Insert ---
df.to_sql("tickers", con=engine, schema="dbo", if_exists="append", index=False)

print("Inserted rows:", len(df))
print(df.head())