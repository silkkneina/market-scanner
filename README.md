# 📊 Market Scanner — Automated Stock Analytics Platform

A production-style stock analytics system that ingests market data, builds a data warehouse, and surfaces actionable insights through an interactive dashboard.

---

## 🚀 Overview

This project is a full end-to-end data pipeline and analytics dashboard designed to:

- Track S&P 500 + broader US equity universe (~12k tickers)
- Detect high-performing stocks using momentum and risk-adjusted metrics
- Identify hidden opportunities outside major indices
- Automate data ingestion and updates daily

---

## 🧠 Key Features

### 📈 Market Analytics
- Daily returns (1D)
- Momentum (5D, 20D, 60D)
- Rolling volatility (20D, annualized)
- Risk-adjusted returns (return / volatility)
- Relative performance vs market average

### 🔍 Discovery Scanner
- Finds non-S&P500 tickers with strong momentum
- Filters noise and highlights meaningful movers
- Designed to detect early-stage outperformers

### ⚙️ Fully Automated Pipeline
- Daily ticker universe refresh
- Daily price ingestion (batched + rate-limited)
- Bi-weekly S&P500 rebalancing
- Resume-safe ingestion using rotation state

---

## 🏗️ Architecture

Data Flow:

Nasdaq Symbol Feed + S&P500 Constituents  
→ Ticker Universe Loader  
→ Price Ingestion (Stooq API)  
→ SQL Server (prices + tickers tables)  
→ Analytical Views (momentum, volatility, ranking)  
→ Streamlit Dashboard  

---

## 🛠️ Tech Stack

- Python (pandas, requests, SQLAlchemy)
- SQL Server (data warehouse & analytics layer)
- Streamlit (interactive dashboard)
- macOS launchd (automation)
- Stooq API (historical data)

---

## 🔄 Data Pipeline

### Daily
refresh_ticker_universe.py  
importdatasql.py  

### Bi-weekly
download_sp500.py  
refresh_sp500.py  

---

## 📊 Dashboard Features

- Top gainers / losers
- Multi-horizon momentum rankings
- Risk-adjusted rankings
- Volatility leaders
- Momentum vs volatility scatter plot
- Per-ticker deep dive (last 12 months)

---

## ▶️ Run Locally

cd market_scanner  
source venv/bin/activate  
streamlit run main.py  

---

## ⚠️ Notes

- Not all tickers return data (depends on Stooq coverage)
- Illiquid or exotic securities are filtered progressively
- Designed for portfolio and learning purposes

---

## 💡 Future Improvements

- Liquidity filters (volume-based)
- Sector-level analysis
- Alerting system (top movers)
- API layer
- Docker deployment

---

## 👤 Author

Guja Chalatashvili  

- Data Analyst (Banking / Finance)
- Strong SQL background
- Transitioning into Python & Data Engineering

---

## ⭐ Why This Project Matters

This project demonstrates:

- End-to-end data pipeline design
- SQL-based analytics engineering
- Python + SQL integration
- Handling real-world messy data
- Building production-style systems
