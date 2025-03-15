WITH liquidity_calc AS (
    SELECT
        DATE(open_time_ts) AS date,
        symbol,
        SUM(quote_asset_volume) / NULLIF(SUM(number_of_trades), 0) AS liquidity_ratio
    FROM {{ ref('stg_binance_klines') }}
    GROUP BY DATE(open_time_ts),
             symbol
)
SELECT * FROM liquidity_calc
