-- 1. Create a virtual warehouse (the compute engine to process your data)
CREATE OR REPLACE WAREHOUSE financial_compute_wh 
    WITH WAREHOUSE_SIZE = 'XSMALL' 
    AUTO_SUSPEND = 60 
    AUTO_RESUME = TRUE;

-- 2. Create your project database
CREATE OR REPLACE DATABASE alpha_financial_db;

-- 3. Move inside your new database
USE DATABASE alpha_financial_db;

-- 4. Create the BRONZE schema layer for your raw, unparsed JSON data
CREATE OR REPLACE SCHEMA bronze;
USE SCHEMA bronze;

Table for Finnhub Live Ticks
CREATE OR REPLACE TABLE bronze_finnhub_ticks (
    ingestion_id UUID DEFAULT UUID_STRING(),
    ingested_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    raw_payload VARIANT
);

-- Table for AllTick Live Ticks
CREATE OR REPLACE TABLE bronze_alltick_ticks (
    ingestion_id UUID DEFAULT UUID_STRING(),
    ingested_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    raw_payload VARIANT
);

Table for MarketAux News Articles
CREATE OR REPLACE TABLE bronze_marketaux_news (
    ingestion_id UUID DEFAULT UUID_STRING(),
    ingested_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    raw_payload VARIANT
);

-- Table for NewsAPI Articles
CREATE OR REPLACE TABLE bronze_newsapi_news (
    ingestion_id UUID DEFAULT UUID_STRING(),
    ingested_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    raw_payload VARIANT
);

-- Table for Alpha Vantage Stock Candles
CREATE OR REPLACE TABLE bronze_alphavantage_candles (
    ingestion_id UUID DEFAULT UUID_STRING(),
    ingested_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    raw_payload VARIANT
);