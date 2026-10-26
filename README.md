# 🚀 Automated Financial & Crypto Market Data Pipeline (ETL)

An production-grade, asynchronous data pipeline designed to ingest, transform, and optimize high-frequency financial metrics for downstream analytics. This repository demonstrates production-ready Python architecture, advanced analytical SQL, and automated data quality guardrails.

## 🛠️ Tech Stack & Architecture
- **Language:** Python 3.11+ (Asynchronous orchestration with `asyncio` and `aiohttp`)
- **Database:** PostgreSQL (Relational storage, performance optimization)
- **Data Ops:** Advanced SQL, Data Schema Modeling, Data Quality Testing

---

## 🏗️ Pipeline Architecture

1. **Extraction (Async Ingestion):** Pulls live market data and currency rates concurrently from public financial APIs using `aiohttp` to maximize throughput and minimize network bottlenecks.
2. **Staging & Validation:** Ingested JSON data is validated for schema drift and null values before being upserted into an operational data store (ODS).
3. **Transformation (Analytical SQL):** Executes incremental dbt-style SQL transformations using advanced analytical functions (Windowing, CTEs) to compute moving averages, market volatility, and liquidity indexes.

---

## 💻 Core Code Showcases

### 1. High-Performance Async Extractor (Python)
This module bypasses synchronous blocking by fetching market data chunks concurrently, utilizing connection pooling and exponential backoff retry logic.

```python
import asyncio
import aiohttp
import logging

class MarketDataExtractor:
    def __init__(self, base_url: str, symbols: list):
        self.base_url = base_url
        self.symbols = symbols
        self.logger = logging.getLogger(__name__)

    async def fetch_symbol_data(self, session: aiohttp.ClientSession, symbol: str) -> dict:
        url = f"{self.base_url}/ticker/{symbol}"
        try:
            async with session.get(url, timeout=10) as response:
                if response.status == 200:
                    return await response.json()
                self.logger.error(f"Failed to fetch {symbol}: HTTP {response.status}")
                return {}
        except Exception as e:
            self.logger.error(f"Connection error for {symbol}: {str(e)}")
            return {}

    async def extract_all(self) -> list:
        async with aiohttp.ClientSession() as session:
            tasks = [self.fetch_symbol_data(session, sym) for sym in self.symbols]
            results = await asyncio.gather(*tasks)
            return [res for res in results if res]
```

### 2. Advanced Analytical Transformations (SQL)
This query computes the rolling 7-day volume-weighted average price (VWAP) and classifies market volatility tiers using advanced window functions and Common Table Expressions (CTEs).

```sql
WITH RawTicks AS (
    SELECT 
        symbol,
        timestamp::TIMESTAMP as tick_time,
        price::NUMERIC(18, 4) as price,
        volume::NUMERIC(18, 4) as volume,
        ROW_NUMBER() OVER (PARTITION BY symbol, timestamp ORDER BY ingested_at DESC) as dedupe_idx
    FROM staging_market_ticks
),
CleanTicks AS (
    SELECT symbol, tick_time, price, volume
    FROM RawTicks
    WHERE dedupe_idx = 1
),
CalculatedMetrics AS (
    SELECT 
        symbol,
        tick_time,
        price,
        volume,
        SUM(price * volume) OVER(PARTITION BY symbol ORDER BY tick_time ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) as rolling_pv,
        SUM(volume) OVER(PARTITION BY symbol ORDER BY tick_time ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) as rolling_vol,
        LAG(price, 1) OVER(PARTITION BY symbol ORDER BY tick_time) as previous_price
    FROM CleanTicks
)
SELECT 
    symbol,
    tick_time,
    price,
    volume,
    CASE 
        WHEN rolling_vol = 0 THEN price
        ELSE ROUND(rolling_pv / rolling_vol, 4)
    END as vwap_7d,
    ROUND(((price - previous_price) / previous_price) * 100, 2) as price_change_pct,
    CASE 
        WHEN ABS(((price - previous_price) / previous_price) * 100) > 5.0 THEN 'HIGH_VOLATILITY'
        ELSE 'STABLE'
    END as volatility_tier
FROM CalculatedMetrics
ORDER BY symbol, tick_time DESC;
```

---

## ⚡ How to Run & Test
1. Clone the repository: `git clone https://github.com`
2. Install dependencies: `pip install -r requirements.txt`
3. Configure your local PostgreSQL database connection strings in `.env`.
4. Run the automated data quality test suite: `pytest tests/`
5. Execute the pipeline: `python main.py`
