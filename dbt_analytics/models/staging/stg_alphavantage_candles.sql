with source_data as (
    select raw_payload
    from {{ source('snowflake_bronze', 'bronze_alphavantage_candles') }}
),

flattened_candles as (
    select
        raw_payload:"meta_data"."2. Symbol"::string as stock_ticker,
        raw_payload:"meta_data"."3. Last Refreshed"::timestamp as api_refreshed_at,

        f.key::date as candle_date,

        f.value:"open"::float as open_price,
        f.value:"high"::float as high_price,
        f.value:"low"::float as low_price,
        f.value:"close"::float as close_price,
        f.value:"volume"::bigint as trading_volume

    from source_data,
            lateral flatten(input => raw_payload:"time_series") f
)

select *
from flattened_candles
qualify row_number() over (
    partition by stock_ticker, candle_date
    order by api_refreshed_at desc
) = 1