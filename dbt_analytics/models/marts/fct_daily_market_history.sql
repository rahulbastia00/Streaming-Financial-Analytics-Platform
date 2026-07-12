{{ config(
    materialized='table'
) }}

with historical_source as (
    select
        stock_ticker,
        candle_date,
        open_price,
        high_price,
        low_price,
        close_price,
        trading_volume,
        api_refreshed_at
    from 
        {{ ref('stg_alphavantage_candles') }}
)

select
    -- 1. Foreign Key link to dim_assets
    md5(stock_ticker) as asset_key,
    
    -- 2. Dimension Keys
    stock_ticker,
    candle_date,
    
    -- 3. OHLCV Candlestick Metrics
    open_price,
    high_price,
    low_price,
    close_price,
    trading_volume,
    
    -- 4. Analytical Derivatives: High-Low daily spread volatility window
    (high_price - low_price)::float as daily_price_range,
    
    api_refreshed_at
from 
    historical_source