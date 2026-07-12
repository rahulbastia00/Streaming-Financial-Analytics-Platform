# Enterprise Data Pipeline Architecture Documentation

**Architecture Style:** Medallion Architecture (Bronze $\rightarrow$ Silver $\rightarrow$ Intermediate $\rightarrow$ Gold)

**Primary Destinations:** Power BI Executive Dashboards & Hugging Face Machine Learning Inference Engines (`ProsusAI/finbert` and `amazon/chronos-t5-small`)

---

## 1. System Overview & Engineering Philosophy

This data pipeline transforms raw, high-velocity financial streams (WebSockets and REST APIs) into structured analytical models. The architecture strictly decouples **data extraction** from **business logic** using dbt inside Snowflake.

Every transformation layer serves a specific architectural barrier:

- **Bronze Layer:** Raw data preservation and schema-on-read landing.
- **Silver Staging Layer:** JSON flattening, data typing, and infrastructure deduplication.
- **Intermediate Conformed Layer:** Schema harmonization and cross-source unification.
- **Gold Analytics & ML Layer:** Star-schema dimensional modeling and machine learning feature engineering.

---

## 2. Bronze Layer: Raw Landing Zone

The Bronze layer acts as the immutable historical archive. Data arrives from five distinct Python producer scripts pushing messages through Kafka into Snowflake tables.

- **Data Structure:** Semi-structured JSON payloads stored directly inside a native Snowflake `VARIANT` column named `raw_payload`.
- **Engineering Challenge Handled:** Because ingestion scripts (`run_all.py`) execute repeatedly during development and batch backfills, the Bronze layer intentionally absorbs duplicate records and schema variations without failing or dropping data.

---

## 3. Silver Layer: Staging & Clean-Up (`models/staging/`)

The Silver layer takes isolated raw tables and transforms them into clean, relational SQL views. Every model in this layer processes exclusively **one source table at a time** without joining across different data streams.

### Universal Staging Transformations

Across all five staging models, three core data engineering patterns were enforced:

1. **Millisecond Epoch Conversion:** Financial APIs (AllTick and Finnhub) transmit timestamps as 13-digit integers. These were converted to human-readable timestamps using `to_timestamp_ntz(raw_payload:time_field::numeric / 1000)`.
2. **Dynamic JSON Exploding:** Nested arrays and dictionaries were unpacked into independent relational rows using Snowflake's native `LATERAL FLATTEN` table function.
3. **Infrastructure Deduplication:** To strip out duplicate messages created by multiple script runs, every staging model utilizes an analytical window function guardrail:

```sql
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY unique_composite_key
    ORDER BY timestamp_field DESC
) = 1

```

### Staging Model Specifications

| Staging Model                  | Source API              | Key Structural Transformations Applied                                                                                                                                                                         |
| ------------------------------ | ----------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **`stg_alltick_ticks`**        | AllTick WebSocket       | Filtered for live market depth updates using `WHERE raw_payload:cmd_id = 22999`. Extracted the top order book layer by targeting index zero of the nested arrays (`bids[0]` and `asks[0]`).                    |
| **`stg_alphavantage_candles`** | Alpha Vantage Daily API | Exploded the dynamic date dictionary (`"Time Series (Daily)"`) using `LATERAL FLATTEN` to convert text date keys into explicit database rows. Deduplicated by `stock_ticker` and `candle_date`.                |
| **`stg_finnhub_ticks`**        | Finnhub WebSocket       | Unpacked high-frequency, single-letter compression schemas (`s` $\rightarrow$ ticker, `p` $\rightarrow$ price, `v` $\rightarrow$ volume). Preserved exchange trade condition codes as native Snowflake arrays. |
| **`stg_marketaux_news`**       | MarketAux News API      | Used `LATERAL FLATTEN` on the `entities` array so articles mentioning multiple companies clone the core headline metadata and generate one distinct row per ticker symbol.                                     |
| **`stg_newsapi_news`**         | NewsAPI REST Service    | Flattened nested author and source dictionary fields (`source.name`). Deduplicated using a composite key of `(title, published_timestamp)`.                                                                    |

---

## 4. Intermediate Layer: Schema Conformance (`models/intermediate/`)

The Intermediate layer solves structural mismatches between different API providers, combining identical data domains into unified SQL tables before they hit the business reporting layer.

### `int_unified_news.sql` (Global Text Stream)

Unifies **MarketAux** (structured financial news) and **NewsAPI** (unstructured general news) into a single conformed table.

- **Synthetic Primary Keys:** Because NewsAPI lacks a UUID, a deterministic hash key was generated using `md5(concat(title, published_timestamp))`.
- **Dynamic Keyword NLP Tagging:** NewsAPI provides no ticker tags. A SQL `CASE WHEN ... ILIKE` engine scans headline and description strings for corporate keywords (e.g., `"Nvidia"`, `"Bitcoin"`, `"MSFT"`) and tags them with our standardized portfolio symbols.
- **Language & Sentiment Fallbacks:** Applied `'en'` as the default language for NewsAPI and injected a baseline float placeholder (`0.0000::float`) for missing sentiment scores, preparing the text for downstream machine learning.

### `int_unified_market_ticks.sql` (Global Price Stream)

Unifies **AllTick** (24/7 crypto and forex order books) and **Finnhub** (Monday–Friday US equities) into a single high-frequency transaction ledger.

- **Granularity Harmonization (The Mid-Price Solution):** Finnhub streams trade executions (`trade_price`), while AllTick streams order book depth (`best_bid` and `best_ask`). To unify them without losing financial accuracy, AllTick's execution price was dynamically derived using the spread midpoint:

```sql
((best_bid_price + best_ask_price) / 2.0)::float AS trade_price

```

- **Volume Aggregation:** Combined AllTick's bid and ask depth (`best_bid_volume + best_ask_volume`) to represent total liquidity volume matching Finnhub's trade volume schema.

---

## 5. Gold Layer: Data Marts & ML Feature Store (`models/marts/`)

The Gold layer projects the clean, conformed data into specialized schemas designed for immediate consumption by Power BI visual dashboards and open-source Hugging Face neural networks.

### Dimensional Lookup Foundation

- **`dim_assets.sql`:** A wide dimension table acting as the central lookup authority for the platform's 8 target portfolio assets (4 US equities, 4 crypto pairs). It generates a deterministic primary key (`md5(asset_ticker) AS asset_key`) used to drive Power BI star-schema relationships, mapping ticker strings to full corporate names, sectors, trading hours, and risk profiles.

### Power BI Analytics Marts

- **`fct_realtime_market_stream.sql`:** A narrow, high-performance transactional fact table optimized for sub-second Power BI dashboard loading. It houses the continuous live ticker tape from `int_unified_market_ticks` and links directly to `dim_assets` via `asset_key`.
- **`fct_daily_market_history.sql`:** Isolates the **Alpha Vantage** daily candlestick data (our architectural outlier). It prevents high-frequency microsecond ticks from distorting long-term macro analysis by providing Power BI with a dedicated, lightweight source for rendering 100-day historical trendlines and calculating daily price range volatility (`high_price - low_price`).

### Hugging Face Machine Learning Feature Store

- **`fct_nlp_feature_inputs.sql` (Feeding `ProsusAI/finbert`):**
- **Purpose:** Pre-processes news streams for natural language processing inference.
- **Engineering:** Filters strictly for English text (`WHERE language = 'en'`) to prevent tokenization hallucinations in FinBERT. Concatenates the title and body description into a single, high-density text block (`nlp_text_payload`) formatted explicitly for BERT's 512-token input limit.

- **`fct_market_ml_features.sql` (Feeding `amazon/chronos-t5-small`):**
- **Purpose:** Constructs continuous, gapless numerical time-series sequences required by Amazon's zero-shot forecasting model.
- **Engineering:** Executes a **Time-Windowed Left Join** that attaches Alpha Vantage's 100-day historical macro metrics directly onto incoming live streaming ticks from Finnhub and AllTick. It computes a rolling 20-day Simple Moving Average (`sma_20`) and historical standard deviation (`rolling_volatility`), outputting a real-time percentage deviation score:

```sql
((live_price - sma_20) / nullif(sma_20, 0) * 100.0)::float AS price_deviation_pct

```

---

## 6. Target Destination Matrix

The entire pipeline converges into five production-ready Gold tables, each tailored for a specific downstream reading engine:

| Gold Model Name                  | Schema Type       | Primary Reading Engine      | Key Output Metrics & Features                                    |
| -------------------------------- | ----------------- | --------------------------- | ---------------------------------------------------------------- |
| **`dim_assets`**                 | Dimension         | Power BI / ML Lookup        | `asset_key`, `asset_name`, `sector_theme`, `risk_profile`        |
| **`fct_realtime_market_stream`** | Fact (Live)       | Power BI Dashboard          | Sub-second ticker tape, execution prices, volume spikes          |
| **`fct_daily_market_history`**   | Fact (Historical) | Power BI Dashboard          | 100-day OHLCV candles, macro volatility ranges                   |
| **`fct_nlp_feature_inputs`**     | Feature Store     | Hugging Face (`finbert`)    | Clean English token strings, baseline sentiment scores           |
| **`fct_market_ml_features`**     | Feature Store     | Hugging Face (`chronos-t5`) | Live price deviation %, 20-day SMA baselines, rolling volatility |

This architecture ensures that Power BI reporting visualizers and Hugging Face inference scripts never perform raw data parsing, string manipulation, or complex window math on the fly. All computational heavy lifting is pre-compiled inside Snowflake, delivering instant dashboard rendering and rapid machine learning execution.
