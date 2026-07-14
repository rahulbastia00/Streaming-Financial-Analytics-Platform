with raw_source as (
    select 
        raw_payload
    from 
        {{ source('snowflake_bronze', 'bronze_alltick_ticks') }}
    where 
        -- Guardrail: Only ingest valid market depth updates, filtering out heartbeats
        raw_payload:cmd_id::int = 22999
)

select
    -- Extract Asset Identity
    raw_payload:data.code::string as asset_ticker,

    -- Time Operations: Convert 13-digit millisecond Unix epoch to a clean timestamp
    to_timestamp_ntz(raw_payload:data.tick_time::numeric / 1000) as event_timestamp,

    -- Flatten the Nested Bids Array (Extracting the top item: Index 0)
    raw_payload:data.bids[0].price::float as best_bid_price,
    raw_payload:data.bids[0].volume::float as best_bid_volume,

    -- Flatten the Nested Asks Array (Extracting the top item: Index 0)
    raw_payload:data.asks[0].price::float as best_ask_price,
    raw_payload:data.asks[0].volume::float as best_ask_volume,

    -- Track Message Lineage Order
    raw_payload:data.seq::bigint as sequence_id

from 
    raw_source

-- Infrastructure Deduplication Guardrail
qualify row_number() over (
    partition by asset_ticker, sequence_id 
    order by event_timestamp desc
) = 1