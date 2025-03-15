WITH base_data AS (
    SELECT
        DATE(open_time_ts) AS date,
        symbol,
        high_price,
        low_price,
        open_price,
        FIRST_VALUE(open_price) OVER (PARTITION BY symbol, DATE(open_time_ts) ORDER BY open_time_ts) AS first_open_price
    FROM {{ ref('stg_binance_klines') }}
),
volatility_calc AS (
    SELECT
        date,
        symbol,
        ((MAX(high_price) - MIN(low_price)) / MAX(first_open_price)) * 100 AS volatility
    FROM base_data
    GROUP BY date, symbol
)
SELECT * FROM volatility_calc
