WITH source AS (
    SELECT
        open_time_ts,
        close_time_ts,
        open_price,
        high_price,
        low_price,
        close_price,
        volume,
        quote_asset_volume,
        number_of_trades,
        taker_buy_base_asset_volume,
        taker_buy_quote_asset_volume,
        'BTCBRL' AS symbol
    FROM {{ source('raw', 'raw_binance_klines_BTCBRL') }}
)

SELECT * FROM source
