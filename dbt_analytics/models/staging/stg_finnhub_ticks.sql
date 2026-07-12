with source_data as (
    select raw_payload
    from {{source('snowflake_bronze', 'bronze_finnhub_ticks')}}
),

parsed_tick as (
    select
        raw_payload:s::string as stock_ticker,
        to_timestamp_ntz(raw_payload:t::numeric / 1000) as event_timestamp,
        raw_payload:v::float as trade_volume,
        raw_payload:p::float as trade_price,
        raw_payload:c as trade_conditions
    
    from 
        source_data
    where
        raw_payload:s is not null
) 
select 
    stock_ticker,
    event_timestamp,
    trade_price,
    trade_volume,
    trade_conditions
from 
    parsed_tick
-- DataOps Resilience: Filter out pipeline infrastructure duplicates
qualify row_number() over (
    partition by stock_ticker, event_timestamp, trade_price, trade_volume 
    order by event_timestamp desc
) = 1
