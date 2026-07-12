{{ config(
    materialized='table'
) }}

with marketaux_source as (
    select
        -- Unique Identifier
        article_uuid as news_id,
        entity_ticker as asset_ticker,
        sentiment_score::float as sentiment_score,
        
        -- Core Text Data
        title,
        description,
        snippet as content_body,
        
        -- Publishing Details
        null::varchar as author,
        news_source,
        null::varchar as image_url,
        
        -- Language Feature (Preserved from MarketAux source)
        language,
        
        published_timestamp,
        
        -- Metadata Lineage tag
        'MarketAux' as api_provider
    from 
        {{ ref('stg_marketaux_news') }}
),

newsapi_source as (
    select
        -- 1. Construct a deterministic unique string key for NewsAPI rows
        md5(concat(title, published_timestamp)) as news_id,
        
        -- 2. Scan content vectors dynamically for target portfolio tags
        case 
            when title ilike '%bitcoin%' or description ilike '%bitcoin%' or title ilike '%btc%' then 'BTCUSDT'
            when title ilike '%ethereum%' or description ilike '%ethereum%' or title ilike '%eth%' then 'ETHUSDT'
            when title ilike '%solana%' or description ilike '%solana%' or title ilike '%sol%' then 'SOLUSDT'
            when title ilike '%ripple%' or description ilike '%ripple%' or title ilike '%xrp%' then 'XRPUSDT'
            when title ilike '%nvidia%' or description ilike '%nvidia%' or title ilike '%nvda%' then 'NVDA'
            when title ilike '%microsoft%' or description ilike '%microsoft%' or title ilike '%msft%' then 'MSFT'
            when title ilike '%google%' or description ilike '%google%' or title ilike '%googl%' then 'GOOGL'
            when title ilike '%amazon%' or description ilike '%amazon%' or title ilike '%amzn%' then 'AMZN'
            else 'UNKNOWN_PORTFOLIO'
        end as asset_ticker,
        
        -- 3. Static float placeholder for downstream FinBERT model scoring
        0.0000::float as sentiment_score,
        
        -- Core Text Data
        title,
        description,
        content as content_body,
        
        -- Publishing Details
        author,
        source_name as news_source,
        image_url,
        
        -- Language Feature (Defaulted to English fallback for NewsAPI)
        'en' as language,
        
        published_timestamp,
        
        -- Metadata Lineage tag
        'NewsAPI' as api_provider
    from 
        {{ ref('stg_newsapi_news') }}
)

-- Synchronized Union Execution
select * from marketaux_source

union all

select * from newsapi_source
where asset_ticker != 'UNKNOWN_PORTFOLIO' -- Exclude noise articles