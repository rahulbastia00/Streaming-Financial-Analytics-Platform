{{ config(
    materialized='table'
) }}

with live_stream as (
    select
        stock_ticker,
        event_timestamp,
        trade_price,
        trade_volume
    from 
        {{ ref('int_unified_market_ticks') }}
),

historical_baselines as (
    select
        stock_ticker,
        close_price as yesterday_close,
        -- Calculate 20-day Simple Moving Average (SMA) as an inference baseline
        avg(close_price) over (
            partition by stock_ticker 
            order by candle_date asc 
            rows between 19 preceding and current row
        )::float as sma_20,
        -- Calculate historical 20-day price volatility
        stddev(close_price) over (
            partition by stock_ticker 
            order by candle_date asc 
            rows between 19 preceding and current row
        )::float as historical_volatility_20d
    from 
        {{ ref('stg_alphavantage_candles') }}
    -- Isolate the latest completed historical day to join against live ticks
    qualify row_number() over (
        partition by stock_ticker 
        order by candle_date desc
    ) = 1
)

select
    -- 1. Foreign Key link to dim_assets
    md5(l.stock_ticker) as asset_key,
    l.stock_ticker,
    l.event_timestamp,
    
    -- 2. Live Transaction Metrics
    l.trade_price as live_price,
    l.trade_volume as live_volume,
    
    -- 3. Attached Historical Baselines from Alpha Vantage
    coalesce(h.yesterday_close, l.trade_price) as baseline_close,
    coalesce(h.sma_20, l.trade_price) as sma_20_baseline,
    coalesce(h.historical_volatility_20d, 0.0) as rolling_volatility,
    
    -- 4. Engineered Feature: Real-time percentage deviation from the 20-day moving average
    ((l.trade_price - coalesce(h.sma_20, l.trade_price)) / nullif(coalesce(h.sma_20, l.trade_price), 0) * 100.0)::float as price_deviation_pct,
    
    current_timestamp() as dw_inserted_at
from 
    live_stream l
left join 
    historical_baselines h 
    on l.stock_ticker = h.stock_ticker