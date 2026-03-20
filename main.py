import os
import pandas as pd
import streamlit as st
from sqlalchemy import create_engine
from dotenv import load_dotenv

# -----------------------------
# Page config
# -----------------------------
st.set_page_config(page_title="Stock Dashboard", layout="wide")
st.title("Stock Dashboard")

# -----------------------------
# DB connection
# -----------------------------
load_dotenv()

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "1433")
DB_NAME = os.getenv("DB_NAME", "MarketData")
DB_USER = os.getenv("DB_USER", "sa")
DB_PASSWORD = os.getenv("DB_PASSWORD")

if not DB_PASSWORD:
    st.error("DB_PASSWORD is missing in your .env file.")
    st.stop()

engine = create_engine(
    f"mssql+pymssql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}",
    echo=False,
)

def esc_sql(s: str) -> str:
    return (s or "").replace("'", "''")

# -----------------------------
# Cached loaders
# -----------------------------
@st.cache_data(ttl=600)
def load_dates():
    q = "SELECT DISTINCT [date] FROM dbo.vw_vs_universe_avg ORDER BY [date] DESC"
    df = pd.read_sql(q, con=engine)
    return df["date"].tolist() if not df.empty else []

@st.cache_data(ttl=600)
def load_latest_snapshot():
    q = """
    SELECT TOP 1 [date], ticker_count, universe_avg_return
    FROM dbo.vw_vs_universe_avg
    ORDER BY [date] DESC
    """
    return pd.read_sql(q, con=engine)

@st.cache_data(ttl=600)
def load_day(selected_date):
    d = pd.to_datetime(selected_date).date().isoformat()
    q = f"""
    SELECT
        u.[date],
        u.ticker,
        u.[close],
        u.daily_return,
        u.return_5d,
        u.return_20d,
        u.return_60d,
        u.universe_avg_return,
        u.excess_return_vs_universe,
        u.ticker_count,
        u.vol_20d_annualized,

        d.vol_20d,
        d.vol_60d,
        d.vol_60d_annualized,
        d.momentum_20d_risk_adj,
        d.momentum_60d_risk_adj,
        d.rank_gainers,
        d.rank_losers,
        d.rank_return_5d,
        d.rank_return_20d,
        d.rank_return_60d,
        d.rank_risk_adj_20d,
        d.rank_risk_adj_60d
    FROM dbo.vw_vs_universe_avg u
    LEFT JOIN dbo.vw_dashboard d
        ON u.[date] = d.[date]
       AND u.ticker = d.ticker
    WHERE u.[date] = '{d}'
    """
    return pd.read_sql(q, con=engine)

@st.cache_data(ttl=600)
def load_ticker_history_last_12m(ticker):
    t = esc_sql(str(ticker))
    q = f"""
    SELECT
        u.[date],
        u.ticker,
        u.[close],
        u.daily_return,
        u.return_5d,
        u.return_20d,
        u.return_60d,
        u.universe_avg_return,
        u.excess_return_vs_universe,
        u.ticker_count,
        u.vol_20d_annualized,

        d.vol_20d,
        d.vol_60d,
        d.vol_60d_annualized,
        d.momentum_20d_risk_adj,
        d.momentum_60d_risk_adj,
        d.rank_gainers,
        d.rank_losers,
        d.rank_return_5d,
        d.rank_return_20d,
        d.rank_return_60d,
        d.rank_risk_adj_20d,
        d.rank_risk_adj_60d
    FROM dbo.vw_vs_universe_avg u
    LEFT JOIN dbo.vw_dashboard d
        ON u.[date] = d.[date]
       AND u.ticker = d.ticker
    WHERE u.ticker = '{t}'
      AND u.[date] >= DATEADD(year, -1, CAST(GETDATE() AS date))
    ORDER BY u.[date]
    """
    return pd.read_sql(q, con=engine)

@st.cache_data(ttl=600)
def load_discovery_dates():
    q = "SELECT DISTINCT [date] FROM dbo.vw_discovery_scanner ORDER BY [date] DESC"
    df = pd.read_sql(q, con=engine)
    return df["date"].tolist() if not df.empty else []

@st.cache_data(ttl=600)
def load_discovery_day(selected_date):
    d = pd.to_datetime(selected_date).date().isoformat()
    q = f"""
    SELECT *
    FROM dbo.vw_discovery_scanner
    WHERE [date] = '{d}'
    """
    return pd.read_sql(q, con=engine)

# -----------------------------
# Helpers
# -----------------------------
def to_numeric_safe(df, cols):
    for col in cols:
        if col in df.columns:
            if isinstance(df[col], pd.DataFrame):
                df[col] = pd.to_numeric(df[col].iloc[:, 0], errors="coerce")
            else:
                df[col] = pd.to_numeric(df[col], errors="coerce")
    return df

main_num_cols = [
    "close", "daily_return", "return_5d", "return_20d", "return_60d",
    "universe_avg_return", "excess_return_vs_universe", "ticker_count",
    "vol_20d", "vol_20d_annualized", "vol_60d", "vol_60d_annualized",
    "momentum_20d_risk_adj", "momentum_60d_risk_adj",
    "rank_gainers", "rank_losers", "rank_return_5d", "rank_return_20d",
    "rank_return_60d", "rank_risk_adj_20d", "rank_risk_adj_60d",
]

discovery_num_cols = [
    "close", "daily_return", "return_5d", "return_20d", "return_60d",
    "vol_20d", "vol_20d_annualized", "vol_60d", "vol_60d_annualized",
    "momentum_20d_risk_adj", "momentum_60d_risk_adj",
    "rank_return_5d", "rank_return_20d", "rank_return_60d",
    "rank_risk_adj_20d", "rank_risk_adj_60d", "avg_close_20d",
]

# -----------------------------
# Load dates + snapshot
# -----------------------------
dates = load_dates()
if not dates:
    st.error("No data found in dbo.vw_vs_universe_avg.")
    st.stop()

latest_df = load_latest_snapshot()
if latest_df.empty:
    st.error("No data found in dbo.vw_vs_universe_avg.")
    st.stop()

latest = latest_df.iloc[0]
st.caption(
    f"Latest date: {latest['date']} | tickers in universe: {int(latest['ticker_count'])} | "
    f"universe avg return: {float(latest['universe_avg_return']) * 100:.2f}%"
)

tab1, tab2 = st.tabs(["Main Dashboard", "Discovery Scanner"])

# =========================================================
# TAB 1 - MAIN DASHBOARD
# =========================================================
with tab1:
    c1, c2, c3 = st.columns([2, 3, 3])

    with c1:
        selected_date = st.selectbox("Date", dates, index=0, key="main_date")

    with c2:
        rank_mode = st.selectbox(
            "Rank by",
            [
                "Daily return",
                "5D momentum",
                "20D momentum",
                "60D momentum",
                "Excess vs universe (daily)",
                "Risk-adjusted (daily return / vol)",
                "Risk-adjusted (20D momentum / vol)",
                "Risk-adjusted (60D momentum / vol)",
            ],
            index=0,
            key="main_rank_mode",
        )

    with c3:
        min_vol_filter = st.slider(
            "Min annualized vol filter",
            min_value=0.0,
            max_value=2.0,
            value=0.0,
            step=0.05,
            help="Useful for risk-adjusted rankings.",
            key="main_vol_filter",
        )

    df_day = load_day(selected_date).copy()
    if df_day.empty:
        st.warning("No rows for this date.")
        st.stop()

    df_day = to_numeric_safe(df_day, main_num_cols)

    if min_vol_filter > 0:
        vol_filter_col = "vol_20d_annualized"
        if rank_mode == "Risk-adjusted (60D momentum / vol)" and "vol_60d_annualized" in df_day.columns:
            vol_filter_col = "vol_60d_annualized"
        df_day = df_day[df_day[vol_filter_col] >= min_vol_filter].copy()

    df_day["risk_adj_daily"] = df_day["daily_return"] / df_day["vol_20d_annualized"]
    df_day["risk_adj_20d"] = df_day["return_20d"] / df_day["vol_20d_annualized"]
    df_day["risk_adj_60d"] = df_day["return_60d"] / df_day["vol_60d_annualized"]

    if rank_mode == "Daily return":
        metric_col = "daily_return"
    elif rank_mode == "5D momentum":
        metric_col = "return_5d"
    elif rank_mode == "20D momentum":
        metric_col = "return_20d"
    elif rank_mode == "60D momentum":
        metric_col = "return_60d"
    elif rank_mode == "Excess vs universe (daily)":
        metric_col = "excess_return_vs_universe"
    elif rank_mode == "Risk-adjusted (daily return / vol)":
        metric_col = "risk_adj_daily"
    elif rank_mode == "Risk-adjusted (20D momentum / vol)":
        metric_col = "momentum_20d_risk_adj"
    else:
        metric_col = "momentum_60d_risk_adj"

    df_rank = df_day[df_day[metric_col].notna()].copy()

    display_cols = [
        "ticker", "close", "daily_return", "return_5d", "return_20d", "return_60d",
        "excess_return_vs_universe", "vol_20d_annualized", "vol_60d_annualized",
        "momentum_20d_risk_adj", "momentum_60d_risk_adj",
    ]
    display_cols = [c for c in display_cols if c in df_rank.columns]

    left, right = st.columns(2)
    with left:
        st.subheader("Top 10")
        top10 = df_rank.sort_values(metric_col, ascending=False).head(10)
        st.dataframe(top10[display_cols], use_container_width=True)

    with right:
        st.subheader("Bottom 10")
        bot10 = df_rank.sort_values(metric_col, ascending=True).head(10)
        st.dataframe(bot10[display_cols], use_container_width=True)

    st.subheader("Highest volatility (Top 10)")
    vol10 = df_day.sort_values("vol_20d_annualized", ascending=False).head(10)
    vol_display_cols = [c for c in ["ticker", "close", "vol_20d_annualized", "daily_return", "return_20d"] if c in vol10.columns]
    st.dataframe(vol10[vol_display_cols], use_container_width=True)

    st.subheader("Top risk-adjusted momentum")
    ram = df_day[["ticker", "return_20d", "vol_20d_annualized", "momentum_20d_risk_adj"]].copy()
    ram = ram.dropna()
    ram = ram[ram["vol_20d_annualized"] > 0]
    ram = ram.sort_values("momentum_20d_risk_adj", ascending=False).head(10)
    st.dataframe(ram, use_container_width=True)

    st.subheader("Momentum vs Volatility (scatter)")
    scatter_horizon = st.selectbox("Scatter horizon", ["5D", "20D", "60D"], index=1, key="main_scatter_horizon")
    hcol = {"5D": "return_5d", "20D": "return_20d", "60D": "return_60d"}[scatter_horizon]

    scatter_df = df_day[["ticker", hcol, "vol_20d_annualized"]].copy()
    scatter_df = scatter_df.dropna()
    scatter_df = scatter_df.rename(columns={hcol: "momentum"})
    scatter_df = scatter_df[
        (scatter_df["vol_20d_annualized"] > 0) &
        (scatter_df["vol_20d_annualized"] < 2.0)
    ]

    st.scatter_chart(scatter_df.set_index("ticker"), x="vol_20d_annualized", y="momentum")
    st.caption("Top-right = strong momentum but risky.")

    st.divider()

    tickers = sorted(df_day["ticker"].dropna().unique().tolist())

    cA, cB = st.columns([2, 5])
    with cA:
        query = st.text_input("Search ticker", value="", key="main_search")
        filtered = [t for t in tickers if query.upper() in t.upper()] if query else tickers
        if not filtered:
            st.warning("No tickers match search.")
            st.stop()
        ticker = st.selectbox("Ticker", filtered, index=0, key="main_ticker")

    df_t = load_ticker_history_last_12m(ticker).copy()
    if df_t.empty:
        st.warning("No history found for this ticker.")
        st.stop()

    df_t = to_numeric_safe(df_t, main_num_cols)
    df_t["date"] = pd.to_datetime(df_t["date"])
    df_t = df_t.sort_values("date")

    row = df_day[df_day["ticker"] == ticker]
    if row.empty:
        st.warning("Selected ticker not available on this date.")
        st.stop()
    row = row.iloc[0]

    k1, k2, k3, k4, k5, k6 = st.columns(6)
    k1.metric("Close", f"{row['close']:.2f}")
    k2.metric("1D", f"{row['daily_return'] * 100:.2f}%")
    k3.metric("5D", f"{row['return_5d'] * 100:.2f}%" if pd.notna(row["return_5d"]) else "—")
    k4.metric("20D", f"{row['return_20d'] * 100:.2f}%" if pd.notna(row["return_20d"]) else "—")
    k5.metric("60D", f"{row['return_60d'] * 100:.2f}%" if pd.notna(row["return_60d"]) else "—")
    k6.metric("Vol (20d ann.)", f"{row['vol_20d_annualized'] * 100:.2f}%" if pd.notna(row["vol_20d_annualized"]) else "—")

    st.caption(
        f"Universe avg (daily): {row['universe_avg_return']*100:.2f}% | "
        f"Excess (daily): {row['excess_return_vs_universe']*100:.2f}%"
    )

    k7, k8, k9, k10 = st.columns(4)
    k7.metric("Risk-Adj 20D", f"{row['momentum_20d_risk_adj']:.2f}" if pd.notna(row["momentum_20d_risk_adj"]) else "—")
    k8.metric("Risk-Adj 60D", f"{row['momentum_60d_risk_adj']:.2f}" if pd.notna(row["momentum_60d_risk_adj"]) else "—")
    k9.metric("Rank 20D", f"{int(row['rank_return_20d'])}" if pd.notna(row["rank_return_20d"]) else "—")
    k10.metric("Rank Risk-Adj 20D", f"{int(row['rank_risk_adj_20d'])}" if pd.notna(row["rank_risk_adj_20d"]) else "—")

    st.subheader(f"{ticker} (last 12 months)")

    st.write("Price")
    st.line_chart(df_t.set_index("date")[["close"]])

    st.write("Returns & momentum")
    chart_cols = [c for c in ["daily_return", "return_5d", "return_20d", "return_60d"] if c in df_t.columns]
    st.line_chart(df_t.set_index("date")[chart_cols])

    st.write("Universe vs Excess (daily)")
    uv_cols = [c for c in ["universe_avg_return", "excess_return_vs_universe"] if c in df_t.columns]
    st.line_chart(df_t.set_index("date")[uv_cols])

    st.write("Annualized volatility")
    vol_chart_cols = [c for c in ["vol_20d_annualized", "vol_60d_annualized"] if c in df_t.columns]
    st.line_chart(df_t.set_index("date")[vol_chart_cols])

    st.write("Risk-adjusted momentum")
    risk_chart_cols = [c for c in ["momentum_20d_risk_adj", "momentum_60d_risk_adj"] if c in df_t.columns]
    if risk_chart_cols:
        st.line_chart(df_t.set_index("date")[risk_chart_cols])

# =========================================================
# TAB 2 - DISCOVERY SCANNER
# =========================================================
with tab2:
    discovery_dates = load_discovery_dates()
    if not discovery_dates:
        st.warning("No data found in dbo.vw_discovery_scanner.")
        st.stop()

    d1, d2, d3, d4 = st.columns([2, 3, 2, 2])

    with d1:
        discovery_date = st.selectbox("Date", discovery_dates, index=0, key="discovery_date")

    with d2:
        discovery_rank_mode = st.selectbox(
            "Rank by",
            [
                "5D momentum",
                "20D momentum",
                "60D momentum",
                "Risk-adjusted 20D",
                "Risk-adjusted 60D",
            ],
            index=1,
            key="discovery_rank_mode",
        )

    with d3:
        min_price = st.number_input("Min price", min_value=0.0, value=3.0, step=1.0, key="discovery_min_price")

    with d4:
        min_vol = st.number_input("Min annualized vol", min_value=0.0, value=0.10, step=0.05, key="discovery_min_vol")

    discovery_top_n = st.slider("Top N", 10, 100, 25, 5, key="discovery_top_n")

    df_disc = load_discovery_day(discovery_date).copy()
    if df_disc.empty:
        st.warning("No discovery rows for this date.")
        st.stop()

    df_disc = to_numeric_safe(df_disc, discovery_num_cols)

    if "close" in df_disc.columns:
        df_disc = df_disc[df_disc["close"] >= min_price].copy()

    if "vol_20d_annualized" in df_disc.columns:
        df_disc = df_disc[df_disc["vol_20d_annualized"] >= min_vol].copy()

    if discovery_rank_mode == "5D momentum":
        disc_metric = "return_5d"
    elif discovery_rank_mode == "20D momentum":
        disc_metric = "return_20d"
    elif discovery_rank_mode == "60D momentum":
        disc_metric = "return_60d"
    elif discovery_rank_mode == "Risk-adjusted 20D":
        disc_metric = "momentum_20d_risk_adj"
    else:
        disc_metric = "momentum_60d_risk_adj"

    df_disc = df_disc[df_disc[disc_metric].notna()].copy()
    df_disc = df_disc.sort_values(disc_metric, ascending=False)

    st.subheader("Discovery candidates")
    disc_cols = [
        "ticker", "close", "daily_return", "return_5d", "return_20d", "return_60d",
        "vol_20d_annualized", "vol_60d_annualized",
        "momentum_20d_risk_adj", "momentum_60d_risk_adj",
    ]
    disc_cols = [c for c in disc_cols if c in df_disc.columns]
    st.dataframe(df_disc[disc_cols].head(discovery_top_n), use_container_width=True)

    cL, cR = st.columns(2)

    with cL:
        st.subheader("Strongest raw momentum")
        raw_top = df_disc.sort_values("return_20d", ascending=False).head(10)
        raw_cols = [c for c in ["ticker", "close", "return_20d", "vol_20d_annualized"] if c in raw_top.columns]
        st.dataframe(raw_top[raw_cols], use_container_width=True)

    with cR:
        st.subheader("Strongest risk-adjusted")
        risk_top = df_disc.sort_values("momentum_20d_risk_adj", ascending=False).head(10)
        risk_cols = [c for c in ["ticker", "close", "return_20d", "vol_20d_annualized", "momentum_20d_risk_adj"] if c in risk_top.columns]
        st.dataframe(risk_top[risk_cols], use_container_width=True)

    st.subheader("Discovery scatter")
    disc_scatter = df_disc[["ticker", "return_20d", "vol_20d_annualized"]].copy().dropna()
    disc_scatter = disc_scatter[
        (disc_scatter["vol_20d_annualized"] > 0) &
        (disc_scatter["vol_20d_annualized"] < 2.0)
    ]
    disc_scatter = disc_scatter.rename(columns={"return_20d": "momentum"})
    st.scatter_chart(disc_scatter.set_index("ticker"), x="vol_20d_annualized", y="momentum")
    st.caption("Discovery tab focuses on non-core stocks with momentum outside your main universe.")