WITH pre_aggregated AS (
    SELECT
        DATE(open_time_ts) AS date,
        symbol,
        open_price,
        high_price,
        low_price,
        close_price,
        volume,
        number_of_trades,
        FIRST_VALUE(open_price) OVER (
            PARTITION BY symbol, DATE(open_time_ts) 
            ORDER BY open_time_ts
        ) AS first_open_price,
        LAST_VALUE(close_price) OVER (
            PARTITION BY symbol, DATE(open_time_ts) 
            ORDER BY open_time_ts 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS last_close_price
    FROM {{ ref('stg_binance_klines') }}
),
aggregated AS (
    SELECT
        date,
        symbol,
        first_open_price AS open_price,
        MAX(high_price) AS high_price,
        MIN(low_price) AS low_price,
        last_close_price AS close_price,
        SUM(volume) AS volume,
        SUM(number_of_trades) AS number_of_trades
    FROM pre_aggregated
    GROUP BY date, symbol, first_open_price, last_close_price
)
SELECT * FROM aggregated
