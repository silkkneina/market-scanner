import os
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

# =========================
# DB CONFIG
# =========================
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "1433")
DB_NAME = os.getenv("DB_NAME", "MarketData")
DB_USER = os.getenv("DB_USER", "sa")
DB_PASSWORD = os.getenv("DB_PASSWORD")

if not DB_PASSWORD:
    raise ValueError("DB_PASSWORD missing. Put it inside your .env file.")

engine = create_engine(
    f"mssql+pymssql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}",
    echo=False,
)

# =========================
# HELPERS
# =========================
def normalize_stooq_symbol(symbol: str) -> str:
    s = (symbol or "").strip().lower()
    return s if s else ""

def normalize_wiki_symbol(symbol: str):
    s = str(symbol).strip()
    return s if s else None

def normalize_security(name: str):
    s = str(name).strip()
    return s if s else None

# =========================
# LOAD CSV
# =========================
csv_path = Path(__file__).resolve().parent.parent / "data" / "sp500_constituents.csv"

if not csv_path.exists():
    raise FileNotFoundError(f"CSV not found: {csv_path}")

df = pd.read_csv(csv_path)

df = df.rename(
    columns={
        "Symbol": "wiki_symbol",
        "Security": "security",
        "stooq_symbol": "stooq_symbol",
    }
)

required_cols = {"wiki_symbol", "security", "stooq_symbol"}
missing = required_cols - set(df.columns)
if missing:
    raise ValueError(f"CSV missing required columns: {missing}")

df = df[list(required_cols)].copy()

df["wiki_symbol"] = df["wiki_symbol"].apply(normalize_wiki_symbol)
df["security"] = df["security"].apply(normalize_security)
df["stooq_symbol"] = df["stooq_symbol"].apply(normalize_stooq_symbol)

df = df[df["stooq_symbol"] != ""].copy()
df = df.drop_duplicates(subset=["stooq_symbol"]).reset_index(drop=True)

if df.empty:
    raise ValueError("No valid rows found in S&P CSV after cleaning.")

print(f"Loaded S&P CSV rows: {len(df)}")
print(df.head())

# =========================
# REFRESH LOGIC
# =========================
with engine.begin() as conn:
    # reset all current active flags
    conn.execute(text("""
        UPDATE dbo.tickers
        SET
            is_active = 0,
            updated_at = SYSDATETIME()
    """))

    inserted = 0
    updated = 0

    for _, row in df.iterrows():
        params = {
            "stooq_symbol": row["stooq_symbol"],
            "wiki_symbol": row["wiki_symbol"],
            "security": row["security"],
        }

        exists_q = text("""
            SELECT COUNT(*) AS cnt
            FROM dbo.tickers
            WHERE stooq_symbol = :stooq_symbol
        """)
        exists = conn.execute(exists_q, params).scalar()

        if exists and int(exists) > 0:
            conn.execute(
                text("""
                    UPDATE dbo.tickers
                    SET
                        wiki_symbol = :wiki_symbol,
                        security = :security,
                        is_active = 1,
                        download_enabled = 1,
                        updated_at = SYSDATETIME()
                    WHERE stooq_symbol = :stooq_symbol
                """),
                params,
            )
            updated += 1
        else:
            conn.execute(
                text("""
                    INSERT INTO dbo.tickers (
                        stooq_symbol,
                        wiki_symbol,
                        security,
                        is_active,
                        download_enabled,
                        created_at,
                        updated_at
                    )
                    VALUES (
                        :stooq_symbol,
                        :wiki_symbol,
                        :security,
                        1,
                        1,
                        SYSDATETIME(),
                        SYSDATETIME()
                    )
                """),
                params,
            )
            inserted += 1

print("S&P refresh complete.")
print(f"Updated existing rows: {updated}")
print(f"Inserted new rows: {inserted}")
print(f"Total CSV rows processed: {len(df)}")