{{ config(
    materialized='table'
) }}

with alltick_source as (
    select
        -- 1. Align Ticker Name
        asset_ticker as stock_ticker,
        event_timestamp,
        
        -- 2. Calculate Mid-Price from Bid/Ask spread to represent Trade Price
        ((best_bid_price + best_ask_price) / 2.0)::float as trade_price,
        
        -- 3. Combine Bid and Ask volume for total market liquidity volume
        (best_bid_volume + best_ask_volume)::float as trade_volume,
        
        -- 4. Fill missing variant column to match Finnhub schema
        null::variant as trade_conditions,
        
        -- Lineage tracking tag
        'AllTick' as price_source
    from 
        {{ ref('stg_alltick_ticks') }}
),

finnhub_source as (
    select
        stock_ticker,
        event_timestamp,
        trade_price::float as trade_price,
        trade_volume::float as trade_volume,
        trade_conditions,
        'Finnhub' as price_source
    from 
        {{ ref('stg_finnhub_ticks') }}
)

-- Execute a seamless union across both aligned streaming engines
select * from alltick_source

union all

select * from finnhub_source