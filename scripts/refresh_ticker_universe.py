import os
from io import StringIO

import pandas as pd
import requests
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
# SOURCE URLS
# =========================
NASDAQ_LISTED_URL = "https://www.nasdaqtrader.com/dynamic/symdir/nasdaqlisted.txt"
OTHER_LISTED_URL = "https://www.nasdaqtrader.com/dynamic/symdir/otherlisted.txt"

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "text/plain,text/csv;q=0.9,*/*;q=0.8",
}

# =========================
# HELPERS
# =========================
def fetch_pipe_file(url: str) -> pd.DataFrame:
    r = requests.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()

    text_body = (r.text or "").strip()
    if not text_body:
        return pd.DataFrame()

    lines = text_body.splitlines()
    cleaned_lines = [line for line in lines if not line.startswith("File Creation Time:")]
    cleaned_text = "\n".join(cleaned_lines).strip()

    if not cleaned_text:
        return pd.DataFrame()

    return pd.read_csv(StringIO(cleaned_text), sep="|")


def normalize_symbol(symbol):
    if pd.isna(symbol):
        return ""

    s = str(symbol).strip().upper()
    if not s:
        return ""

    disallowed_chars = {"$", "+", "*", "=", "^"}
    if any(ch in s for ch in disallowed_chars):
        return ""

    return s.lower() + ".us"


def clean_security_name(name):
    if pd.isna(name):
        return None

    s = str(name).strip()
    if not s:
        return None

    s_low = s.lower()

    banned_terms = [
        "warrant",
        "warrants",
        "rights",
        "units",
        "unit",
        "preferred",
        "depositary shares",
        "interest in a share",
        "perpetual preferred",
        "test issue",
    ]

    if any(term in s_low for term in banned_terms):
        return None

    return s


def load_nasdaq_listed() -> pd.DataFrame:
    df = fetch_pipe_file(NASDAQ_LISTED_URL)
    if df.empty:
        return df

    needed = ["Symbol", "Security Name", "Test Issue"]
    missing = [c for c in needed if c not in df.columns]
    if missing:
        raise ValueError(f"nasdaqlisted.txt missing expected columns: {missing}")

    df = df[needed].copy()
    df = df[df["Test Issue"] == "N"].copy()

    df["wiki_symbol"] = None
    df["security"] = df["Security Name"].apply(clean_security_name)
    df["stooq_symbol"] = df["Symbol"].apply(normalize_symbol)

    df = df[df["security"].notna()].copy()
    df = df[df["stooq_symbol"] != ""].copy()

    df = df[["stooq_symbol", "wiki_symbol", "security"]]
    df = df.drop_duplicates(subset=["stooq_symbol"]).reset_index(drop=True)
    return df


def load_other_listed() -> pd.DataFrame:
    df = fetch_pipe_file(OTHER_LISTED_URL)
    if df.empty:
        return df

    needed = ["ACT Symbol", "Security Name", "Test Issue"]
    missing = [c for c in needed if c not in df.columns]
    if missing:
        raise ValueError(f"otherlisted.txt missing expected columns: {missing}")

    df = df[needed].copy()
    df = df[df["Test Issue"] == "N"].copy()

    df["wiki_symbol"] = None
    df["security"] = df["Security Name"].apply(clean_security_name)
    df["stooq_symbol"] = df["ACT Symbol"].apply(normalize_symbol)

    df = df[df["security"].notna()].copy()
    df = df[df["stooq_symbol"] != ""].copy()

    df = df[["stooq_symbol", "wiki_symbol", "security"]]
    df = df.drop_duplicates(subset=["stooq_symbol"]).reset_index(drop=True)
    return df


# =========================
# LOAD + COMBINE
# =========================
nasdaq_df = load_nasdaq_listed()
other_df = load_other_listed()

if nasdaq_df.empty and other_df.empty:
    raise ValueError("Both source files returned empty data.")

df = pd.concat([nasdaq_df, other_df], ignore_index=True)
df = df.drop_duplicates(subset=["stooq_symbol"]).reset_index(drop=True)

print(f"Nasdaq-listed rows kept: {len(nasdaq_df)}")
print(f"Other-listed rows kept: {len(other_df)}")
print(f"Combined unique rows: {len(df)}")
print(df.head())

# =========================
# UPSERT INTO dbo.tickers
# =========================
inserted = 0
updated = 0

with engine.begin() as conn:
    for _, row in df.iterrows():
        params = {
            "stooq_symbol": row["stooq_symbol"],
            "wiki_symbol": row["wiki_symbol"],
            "security": row["security"],
        }

        exists = conn.execute(
            text("""
                SELECT COUNT(*) AS cnt
                FROM dbo.tickers
                WHERE stooq_symbol = :stooq_symbol
            """),
            params
        ).scalar()

        if exists and int(exists) > 0:
            conn.execute(
                text("""
                    UPDATE dbo.tickers
                    SET
                        security = COALESCE(:security, security),
                        updated_at = SYSDATETIME()
                    WHERE stooq_symbol = :stooq_symbol
                """),
                params
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
                        0,
                        1,
                        SYSDATETIME(),
                        SYSDATETIME()
                    )
                """),
                params
            )
            inserted += 1

print("Ticker universe refresh complete.")
print(f"Updated existing rows: {updated}")
print(f"Inserted new rows: {inserted}")
print(f"Total processed rows: {len(df)}")