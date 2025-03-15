WITH daily_prices AS (
    SELECT
        DATE(open_time_ts) AS date,
        symbol,
        FIRST_VALUE(close_price) OVER (PARTITION BY symbol ORDER BY open_time_ts) AS first_close_price,
        close_price,
        LAG(close_price) OVER (PARTITION BY symbol ORDER BY open_time_ts) AS previous_close_price
    FROM {{ ref('stg_binance_klines') }}
),
returns_calc AS (
    SELECT
        date,
        symbol,
        SAFE_DIVIDE((close_price - previous_close_price), previous_close_price) * 100 AS daily_return,
        (SAFE_DIVIDE(close_price, first_close_price) - 1) * 100 AS cumulative_return
    FROM daily_prices
)
SELECT * FROM returns_calc
