with source_data as (
    select 
        raw_payload
    from 
        {{ source('snowflake_bronze', 'bronze_marketaux_news') }}
),

flattened_news as (
    select
        raw_payload:uuid::string as article_uuid,
        raw_payload:title::string as title,
        raw_payload:description::string as description,
        raw_payload:snippet::string as snippet,
        raw_payload:url::string as article_url,
        raw_payload:source::string as news_source,
        raw_payload:language::string as language,
        
        -- Convert standard ISO text string to valid timestamp
        raw_payload:published_at::timestamp_tz as published_timestamp,

        -- 2. Explode the array using f.value to isolate individual ticker sentiment rows
        f.value:symbol::string as entity_ticker,
        f.value:name::string as entity_name,
        f.value:sentiment_score::float as sentiment_score

    from 
        source_data,
        LATERAL FLATTEN(input => raw_payload:entities) f
    where 
        raw_payload:uuid is not null
)

select
    article_uuid,
    entity_ticker,
    sentiment_score,
    title,
    description,
    snippet,
    article_url,
    news_source,
    language,
    published_timestamp,
    entity_name
from 
    flattened_news
-- DataOps Resilience: Drop network duplicate streams completely
qualify row_number() over (
    partition by article_uuid, entity_ticker 
    order by published_timestamp desc
) = 1