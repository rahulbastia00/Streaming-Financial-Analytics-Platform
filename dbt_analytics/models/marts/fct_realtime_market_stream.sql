{{ config(
    materialized='table'
) }}

with clean_ticks as (
    select
        stock_ticker,
        event_timestamp,
        trade_price,
        trade_volume,
        price_source
    from 
        {{ ref('int_unified_market_ticks') }}
)

select
    -- 1. Foreign Key link to dim_assets
    md5(stock_ticker) as asset_key,
    
    -- 2. Transactional Metrics
    stock_ticker,
    event_timestamp,
    trade_price,
    trade_volume,
    price_source,
    
    -- 3. Ingestion tracking timestamp
    current_timestamp() as dw_inserted_at
from 
    clean_ticks