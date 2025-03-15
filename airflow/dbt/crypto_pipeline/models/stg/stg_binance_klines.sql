WITH unioned AS (
    SELECT * FROM {{ ref('stg_binance_klines_BTCBRL') }}
    UNION ALL
    SELECT * FROM {{ ref('stg_binance_klines_ETHBRL') }}
    UNION ALL
    SELECT * FROM {{ ref('stg_binance_klines_SOLBRL') }}
)

SELECT * FROM unioned
