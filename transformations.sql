-- Production Analytical Transformation Pipeline
-- Purpose: Deduplicate raw ticks, compute 7-day volume-weighted average price (VWAP), and flag volatility tiers.

WITH RawTicks AS (
    SELECT 
        symbol,
        timestamp::TIMESTAMP as tick_time,
        price::NUMERIC(18, 4) as price,
        volume::NUMERIC(18, 4) as volume,
        ROW_NUMBER() OVER (PARTITION BY symbol, timestamp ORDER BY timestamp DESC) as dedupe_idx
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
