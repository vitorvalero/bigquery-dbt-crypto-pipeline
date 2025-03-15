WITH price_data AS (
    SELECT
        DATE(open_time_ts) AS date,
        symbol,
        close_price
    FROM {{ ref('stg_binance_klines') }}
),
row_number_calc AS (
    SELECT
        date,
        symbol,
        close_price,
        ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY date) AS row_num
    FROM price_data
),
trend_calc AS (
    SELECT
        date,
        symbol,
        AVG(close_price) OVER (
            PARTITION BY symbol
            ORDER BY date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS sma_7,
        AVG(close_price) OVER (
            PARTITION BY symbol
            ORDER BY date
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) AS sma_30,
        SUM(close_price * EXP(-0.2 * (row_num - 1))) OVER (
            PARTITION BY symbol
            ORDER BY date
        ) / SUM(EXP(-0.2 * (row_num - 1))) OVER (
            PARTITION BY symbol
            ORDER BY date
        ) AS ema_7,
        SUM(close_price * EXP(-0.07 * (row_num - 1))) OVER (
            PARTITION BY symbol
            ORDER BY date
        ) / SUM(EXP(-0.07 * (row_num - 1))) OVER (
            PARTITION BY symbol
            ORDER BY date
        ) AS ema_30
    FROM row_number_calc
)
SELECT * FROM trend_calc
