with source_data as (
    select 
        raw_payload
    from 
        {{ source('snowflake_bronze', 'bronze_newsapi_news') }}
),

parsed_news as (
    select
        raw_payload:title::string as title,
        raw_payload:author::string as author,
        raw_payload:description::string as description,
        raw_payload:content::string as content,
        
        raw_payload:source.id::string as source_id,
        raw_payload:source.name::string as source_name,
        
        raw_payload:url::string as article_url,
        raw_payload:urlToImage::string as image_url,
        
        -- Convert standard text timestamp to valid database timestamp object
        raw_payload:publishedAt::timestamp_tz as published_timestamp

    from 
        source_data
    where 
        raw_payload:title is not null
)

select
    title,
    author,
    description,
    content,
    source_id,
    source_name,
    article_url,
    image_url,
    published_timestamp
from 
    parsed_news
-- DataOps Resilience: Discard duplicate article drops completely
qualify row_number() over (
    partition by title, published_timestamp 
    order by published_timestamp desc
) = 1