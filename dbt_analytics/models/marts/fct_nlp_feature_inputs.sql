{{ config(
    materialized='table'
) }}

with clean_news as (
    select
        news_id,
        asset_ticker,
        -- Concatenate title and description into a clean text payload for FinBERT
        trim(concat(title, ' - ', coalesce(description, content_body, ''))) as nlp_text_payload,
        sentiment_score,
        news_source,
        published_timestamp,
        api_provider
    from 
        {{ ref('int_unified_news') }}
    where 
        language = 'en'
        and title is not null
        and asset_ticker != 'UNKNOWN_PORTFOLIO'
)

select
    -- 1. Foreign Key link to dim_assets
    md5(asset_ticker) as asset_key,
    
    -- 2. Core NLP Feature Inputs
    news_id,
    asset_ticker,
    nlp_text_payload,
    sentiment_score as baseline_sentiment,
    
    -- 3. Lineage & Timing
    news_source,
    published_timestamp,
    current_timestamp() as dw_inserted_at
from 
    clean_news