{{ config(
    materialized='table'
) }}

with asset_reference as (
    select * from (
        values
            ('BTCUSDT', 'Bitcoin', 'Cryptocurrency', 'Digital Gold / Layer 1', '24/7 Global', 'High Volatility'),
            ('ETHUSDT', 'Ethereum', 'Cryptocurrency', 'Smart Contracts / Layer 1', '24/7 Global', 'High Volatility'),
            ('SOLUSDT', 'Solana', 'Cryptocurrency', 'High-Speed Layer 1', '24/7 Global', 'Extreme Volatility'),
            ('XRPUSDT', 'Ripple', 'Cryptocurrency', 'Cross-Border Payments', '24/7 Global', 'High Volatility'),
            ('NVDA', 'Nvidia Corporation', 'US Equity', 'Semiconductors / AI Hardware', 'NASDAQ (Mon-Fri)', 'Growth / Moderate Volatility'),
            ('MSFT', 'Microsoft Corporation', 'US Equity', 'Cloud Computing / Enterprise AI', 'NASDAQ (Mon-Fri)', 'Blue Chip / Stable'),
            ('GOOGL', 'Alphabet Inc.', 'US Equity', 'Search / Cloud / AI Services', 'NASDAQ (Mon-Fri)', 'Blue Chip / Stable'),
            ('AMZN', 'Amazon.com Inc.', 'US Equity', 'E-Commerce / Cloud Infrastructure', 'NASDAQ (Mon-Fri)', 'Blue Chip / Moderate Volatility')
    ) as t(asset_ticker, asset_name, asset_class, sector_theme, trading_hours, risk_profile)
)

select
    -- 1. Create a deterministic Primary Key for Power BI relationships
    md5(asset_ticker) as asset_key,
    
    -- 2. Core Attributes
    asset_ticker,
    asset_name,
    asset_class,
    sector_theme,
    trading_hours,
    risk_profile,
    
    -- 3. Audit Timestamp
    current_timestamp() as created_at
from 
    asset_reference