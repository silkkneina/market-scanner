import os
import time
from datetime import timedelta
from io import StringIO

import pandas as pd
import requests
from dotenv import load_dotenv
from sqlalchemy import create_engine

# =========================
# CONFIG
# =========================
INITIAL_LOAD = True

BATCH_SIZE_INITIAL = 200
BATCH_SIZE_DAILY = 50
SLEEP_SECONDS = 0.7
DEBUG_PRINT = True

KEEP_DAYS = 365
DOWNLOAD_DAYS_IF_NEW = 365

# =========================
# DB SETUP
# =========================
load_dotenv()

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "1433")
DB_NAME = os.getenv("DB_NAME", "MarketData")
DB_USER = os.getenv("DB_USER", "sa")
DB_PASSWORD = os.getenv("DB_PASSWORD")

if not DB_PASSWORD:
    raise ValueError("DB_PASSWORD missing in .env")

engine = create_engine(
    f"mssql+pymssql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}",
    echo=False,
)

def esc_sql(s: str) -> str:
    return (s or "").replace("'", "''")

# =========================
# HTTP SETUP
# =========================
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.6 Safari/605.1.15",
    "Accept": "text/csv,text/plain;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://stooq.com/",
    "Connection": "keep-alive",
}

SESSION = requests.Session()
SESSION.headers.update(HEADERS)

class StooqDailyLimit(Exception):
    pass

def candidate_symbols(raw: str) -> list[str]:
    s = (raw or "").strip()
    if not s:
        return []

    s_low = s.lower()
    s_up = s.upper()

    cands = [s_low, s_up]

    if "." not in s_low:
        cands += [f"{s_low}.us", f"{s_up}.US"]

    seen = set()
    out = []
    for x in cands:
        if x not in seen:
            seen.add(x)
            out.append(x)
    return out

def build_url(symbol: str, start_yyyymmdd: str | None = None) -> str:
    if start_yyyymmdd:
        return f"https://stooq.com/q/d/l/?s={symbol}&i=d&d1={start_yyyymmdd}"
    return f"https://stooq.com/q/d/l/?s={symbol}&i=d"

def fetch_stooq_csv(url: str) -> pd.DataFrame:
    r = SESSION.get(url, timeout=30)
    body = (r.text or "").strip()
    low = body.lower()

    if DEBUG_PRINT:
        print(f"HTTP {r.status_code} | URL={url}")
        print(f"BODY PREVIEW: {body[:200]}\n")

    if "exceeded the daily hits limit" in low:
        raise StooqDailyLimit("Stooq daily hits limit reached")

    if r.status_code != 200:
        return pd.DataFrame()

    if not body:
        return pd.DataFrame()

    if "<html" in low or "forbidden" in low or "access denied" in low:
        return pd.DataFrame()

    if "warning: mysql_num_rows()" in low:
        return pd.DataFrame()

    if "no data" in low or "not found" in low:
        return pd.DataFrame()

    if not low.startswith("date,"):
        return pd.DataFrame()

    return pd.read_csv(StringIO(body))

def fetch_stooq_with_fallback(symbol: str, start_yyyymmdd: str | None) -> pd.DataFrame:
    if start_yyyymmdd:
        url1 = build_url(symbol, start_yyyymmdd)
        df1 = fetch_stooq_csv(url1)
        if not df1.empty:
            return df1

    url2 = build_url(symbol, None)
    df2 = fetch_stooq_csv(url2)
    return df2

# =========================
# ROTATION STATE (SQL)
# =========================
def get_last_symbol() -> str | None:
    q = "SELECT last_symbol FROM dbo.ingest_state WHERE id = 1"
    df = pd.read_sql(q, con=engine)
    if df.empty:
        return None
    return df["last_symbol"].iloc[0]

def set_last_symbol(symbol: str | None) -> None:
    s = "NULL" if symbol is None else f"'{esc_sql(symbol)}'"
    q = f"""
        UPDATE dbo.ingest_state
        SET last_symbol = {s}, updated_at = SYSUTCDATETIME()
        WHERE id = 1
    """
    with engine.begin() as conn:
        conn.exec_driver_sql(q)

def get_batch_size() -> int:
    return BATCH_SIZE_INITIAL if INITIAL_LOAD else BATCH_SIZE_DAILY

def get_next_batch(batch_size: int) -> list[str]:
    last_symbol = get_last_symbol()
    n = int(batch_size)

    if last_symbol:
        last = esc_sql(last_symbol)
        q1 = f"""
            SELECT TOP {n} stooq_symbol
            FROM dbo.tickers
            WHERE download_enabled = 1
              AND stooq_symbol > '{last}'
            ORDER BY stooq_symbol
        """
        batch = pd.read_sql(q1, con=engine)["stooq_symbol"].tolist()

        if len(batch) < n:
            remaining = n - len(batch)
            q2 = f"""
                SELECT TOP {remaining} stooq_symbol
                FROM dbo.tickers
                WHERE download_enabled = 1
                ORDER BY stooq_symbol
            """
            wrap_batch = pd.read_sql(q2, con=engine)["stooq_symbol"].tolist()
            batch.extend(wrap_batch)

        return batch

    q3 = f"""
        SELECT TOP {n} stooq_symbol
        FROM dbo.tickers
        WHERE download_enabled = 1
        ORDER BY stooq_symbol
    """
    return pd.read_sql(q3, con=engine)["stooq_symbol"].tolist()

def get_download_ticker_count() -> int:
    q = """
        SELECT COUNT(*) AS cnt
        FROM dbo.tickers
        WHERE download_enabled = 1
    """
    df = pd.read_sql(q, con=engine)
    return int(df["cnt"].iloc[0])

# =========================
# UPDATE ONE TICKER
# =========================
def get_max_date_for_ticker(ticker: str):
    t = esc_sql(ticker)
    q = f"SELECT MAX([date]) AS MaxDate FROM dbo.prices WHERE ticker = '{t}'"
    df = pd.read_sql(q, con=engine)
    if df.empty:
        return None
    return df["MaxDate"].iloc[0]

def update_ticker(store_ticker: str) -> int:
    store_ticker = (store_ticker or "").strip()
    if not store_ticker:
        print("Empty ticker")
        return 0

    max_date = get_max_date_for_ticker(store_ticker)
    today = pd.Timestamp.today().date()

    if max_date is not None and pd.notna(max_date):
        start_date = pd.to_datetime(max_date).date() + timedelta(days=1)
        if start_date > today:
            print(f"{store_ticker}: already up to date")
            return 0
        start = start_date.strftime("%Y%m%d")
    else:
        cutoff = today - timedelta(days=DOWNLOAD_DAYS_IF_NEW)
        start = cutoff.strftime("%Y%m%d")

    print(f"{store_ticker}: max_date={max_date}, start={start}")

    df = pd.DataFrame()
    used_symbol = None

    for sym in candidate_symbols(store_ticker):
        print(f"{store_ticker}: trying symbol={sym}")

        df_try = fetch_stooq_with_fallback(sym, start)
        print(f"{store_ticker}: symbol={sym}, rows={len(df_try)}")

        if not df_try.empty:
            df = df_try
            used_symbol = sym
            break

    if df.empty:
        print(f"{store_ticker}: no usable data returned from any candidate symbol")
        return 0

    expected = {"Date", "Open", "High", "Low", "Close", "Volume"}
    if not expected.issubset(df.columns):
        print(f"{store_ticker}: bad columns from {used_symbol}: {list(df.columns)}")
        return 0

    df = df.rename(columns={
        "Date": "date",
        "Open": "open",
        "High": "high",
        "Low": "low",
        "Close": "close",
        "Volume": "volume"
    })

    df["ticker"] = store_ticker
    df["date"] = pd.to_datetime(df["date"]).dt.date

    for c in ["open", "high", "low", "close", "volume"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    df = df[["ticker", "date", "open", "high", "low", "close", "volume"]].dropna(subset=["date", "close"])

    if df.empty:
        print(f"{store_ticker}: data became empty after cleaning")
        return 0

    if start:
        start_py = pd.to_datetime(start).date()
        before = len(df)
        df = df[df["date"] >= start_py]
        print(f"{store_ticker}: filtered by start {start_py}, {before} -> {len(df)} rows")

    if max_date is not None and pd.notna(max_date):
        max_d = pd.to_datetime(max_date).date()
        before = len(df)
        df = df[df["date"] > max_d]
        print(f"{store_ticker}: filtered by max_date {max_d}, {before} -> {len(df)} rows")

    if df.empty:
        print(f"{store_ticker}: nothing new to insert")
        return 0

    df.to_sql(
        "prices",
        con=engine,
        schema="dbo",
        if_exists="append",
        index=False,
        chunksize=500
    )

    print(f"{store_ticker}: inserted {len(df)} rows using symbol {used_symbol}")
    return len(df)

# =========================
# CLEANUP
# =========================
def cleanup_prices_keep_last_year():
    q = f"""
        DELETE FROM dbo.prices
        WHERE [date] < DATEADD(day, -{int(KEEP_DAYS)}, CAST(GETDATE() AS date))
    """
    with engine.begin() as conn:
        conn.exec_driver_sql(q)

# =========================
# MAIN RUN
# =========================
download_count = get_download_ticker_count()
batch_size = get_batch_size()
batch = get_next_batch(batch_size)

print(
    f"Mode={'INITIAL_LOAD' if INITIAL_LOAD else 'DAILY'} | "
    f"Download-enabled tickers={download_count} | "
    f"Batch size={len(batch)} | "
    f"last_symbol={get_last_symbol()}"
)
print("Tickers in this run:", batch)

inserted_total = 0
processed = 0

for i, t in enumerate(batch, start=1):
    try:
        n = update_ticker(t)
        inserted_total += n
        processed += 1

        set_last_symbol(t)

        print(f"[{i}/{len(batch)}] {t}: +{n} rows (batch total +{inserted_total})")

    except StooqDailyLimit as e:
        print("\nSTOPPING:", e)
        print("Quota hit. Next run will continue from last saved ticker.")
        break

    except Exception as e:
        processed += 1
        set_last_symbol(t)
        print(f"[{i}/{len(batch)}] {t}: ERROR -> {e}")

    time.sleep(SLEEP_SECONDS)

try:
    cleanup_prices_keep_last_year()
    print(f"Cleanup done: kept last ~{KEEP_DAYS} days in dbo.prices")
except Exception as e:
    print(f"Cleanup ERROR (non-fatal): {e}")

print(
    f"Done. processed={processed}, "
    f"inserted_total={inserted_total}, "
    f"last_symbol={get_last_symbol()}"
)