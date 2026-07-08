# Project Specification Document

## Multi-Modal Real-Time Financial Alpha Generation & Market Sentiment Platform

### Technical Infrastructure Blueprint: Pure Microsoft Fabric Ecosystem

---

## 1. Executive Summary & Business Problem

### The Business Challenge

Financial markets shift rapidly based on a combination of quantitative trade metrics (prices, volumes) and qualitative alternative data (breaking news, corporate announcements, earnings analysis). Traditional trading architectures isolate these streams into distinct silos. This separation introduces processing delays and information asymmetry, preventing analysts and algorithmic models from capturing immediate sentiment-driven market corrections.

### The Technical Solution

This specification outlines a unified, real-time data engineering and MLOps platform hosted entirely within the **Microsoft Fabric** Software-as-a-Service (SaaS) estate. The platform ingests high-frequency stock metrics and unstructured media streams simultaneously, applies real-time Natural Language Processing (NLP) for sentiment assessment, and orchestrates an automated deep learning feedback loop to predict immediate asset movements.

This infrastructure satisfies strict budget limits by running completely within a Fabric Trial tenant, eliminating external cloud infrastructure dependencies while utilizing industry-standard open data protocols.

---

## 2. Integrated Core Technologies Learning Framework

To ensure comprehensive training across enterprise toolsets, the project is structured to directly implement the mechanics of **Apache Kafka**, **Databricks**, and **dbt** utilizing Fabric’s native open-architecture runtimes:

```
[ External Web APIs ]
         │
         ▼ (Kafka API / Protocol Requests)
[ Fabric Eventstream ] ──> Custom App Broker (Apache Kafka Layer)
         │
         ▼ (PySpark Structured Streaming)
[ Synapse Data Engineering ] ──> Fabric Notebooks (Databricks Core Engine Layer)
         │
         ▼ (Delta Parquet File Output)
[ Fabric OneLake Storage ]
         │
         ▼ (T-SQL Analytical Warehouse Access)
[ Fabric dbt Job Runtime ] ──> Integrated SQL Models (dbt Transformation Layer)

```

- **Apache Kafka Layer:** Fabric **Eventstream** features a built-in Custom App gateway that exposes an automated endpoint compatible with the Apache Kafka protocol. Data collection applications interact with this endpoint using standard Python Kafka libraries (`confluent-kafka`), simulating an enterprise-grade distributed message queue.
- **Databricks Core Engine Layer:** Fabric **Synapse Data Engineering Notebooks** utilize an optimized, managed Apache Spark environment built upon **Delta Lake storage formats** and **MLflow experiment tracking**. Because Delta Lake and MLflow are the structural pillars developed by Databricks, writing PySpark streaming and model logging code here builds direct proficiency applicable to any external Databricks workspace.
- **dbt Transformation Layer:** Fabric integrates a fully managed, native **dbt Job Runtime** directly inside the workspace. This enables analytics engineers to develop, test, version-control, and execute modular dbt SQL transformations directly over the data warehouse without managing independent local environments or external compute nodes.

---

## 3. Platform Component Breakdown & Technical Roles

| Architecture Stage                    | Native Fabric Component                  | Technical Implementation & Learning Objective                                                                                                                                                                  |
| ------------------------------------- | ---------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Data Ingestion & Circuit Breaking** | Python Runtime (Local / Virtual Machine) | A lightweight script polls financial endpoints (Finnhub, AllTick, NewsAPI, MarketAux) and executes strict JSON validation. It handles errors and implements a circuit breaker to isolate faulty web resources. |
| **Real-Time Messaging Log**           | **Eventstream** (Custom App Source)      | Operates as the **Apache Kafka Broker**. It exposes standard SASL_SSL endpoints to accept incoming data packages into decoupled topics (`stock-ticks`, `news-feed`).                                           |
| **Streaming Compute Engine**          | **Synapse Data Engineering**             | Operates as the **Databricks Engine**. Spark Structured Streaming queries the Eventstream topics, tokenizes news text arrays using the **FinBERT** NLP model, and outputs clean tables.                        |
| **Centralized Storage Estate**        | **OneLake**                              | The unified storage layer. All outputs are captured as open-source Delta Parquet structures, completely replacing external databases or object storage.                                                        |
| **Analytics & Data Warehouse**        | **Synapse Data Warehouse**               | Provides a relational T-SQL abstraction layer over the OneLake Delta files, allowing relational querying and schema definition.                                                                                |
| **Transformation & Quality Control**  | **Fabric dbt Job Runtime**               | Leverages **dbt Core / dbt-fabric** to build analytical pipelines (Medallion architecture layers), enforce integrity constraints, and generate lineage logs.                                                   |
| **MLOps & Evaluation Loop**           | **Synapse Data Science**                 | Hosts automated deep learning forecasting models. Uses native **MLflow** integrations to manage historical prediction accuracy audits and handle nightly model fine-tuning.                                    |
| **Orchestration & Automation**        | **Data Factory Pipelines**               | The central scheduler. Manages nightly sequences, executing dbt transformations and triggering machine learning retraining loops at 2:00 AM daily.                                                             |
| **Presentation & Live Visuals**       | **Power BI**                             | Renders live data indicators via **DirectLake Mode**, bypassing SQL layers by loading Delta Parquet files into memory for real-time visualization.                                                             |

---

## 4. Detailed Component Data Flow

```
[ Stock Ticks API ] ──┐
                      ▼
            [ Python Validation App ] ──► (Invalid Payload) ──► [ Eventstream Error Topic ] ──► [ Fabric Activator Email ]
                      │
                      ▼ (Valid Payload via Kafka Protocol)
            [ Eventstream Live Topics (`stock-ticks` / `news-feed`) ]
                      │
                      ▼ (PySpark Structured Streaming)
            [ Synapse Data Engineering Notebook (FinBERT Sentiment Processing) ]
                      │
                      ▼ (Append Delta Parquet Files)
            [ OneLake Storage Base ]
                      │
             ┌────────┴────────┐
             ▼                 ▼
   [ Synapse Warehouse ]   [ Synapse Data Science ]
             │                 │ (Nightly Retraining via MLflow)
             ▼                 ▼
   [ Fabric dbt Runtime ] ──► [ Performance History Tables ]
             │
             ▼ (DirectLake Real-Time Memory Load)
   [ Power BI Live Dashboard ]

```

### Phase A: Collection, Validation, and Circuit Breaking

1. A Python application running in a local environment or secure terminal connects to live WebSocket tickers (Finnhub/AllTick) and REST news streams (NewsAPI/MarketAux).
2. The payload passes through a **Pydantic** data-validation structure. If the schema contains broken fields, rate-limit blocks, or invalid data types, a circuit breaker activates to pause that specific endpoint.
3. Invalid JSON text and error telemetry are dispatched to an `ingestion-errors` topic. Valid data is directed to production tracking topics.

### Phase B: Streaming Ingestion & Real-Time Intelligence

1. The validated streams enter the Fabric **Eventstream** instance using a SASL_SSL encrypted connection profile string.
2. The `ingestion-errors` topic forwards messages directly into a **Fabric Activator (Reflex)** monitoring node. Activator parses the error payload and dispatches an alert email to the operations team detailing the broken API layout.
3. The production streaming data points are pulled into a **Synapse Data Engineering Notebook**. The Spark script processes the textual payloads through the financial sentiment model (**FinBERT**), generating real-time continuous values (Positive/Neutral/Negative) alongside incoming pricing data.

### Phase C: Lakehouse Persistence & Analytical Transformation

1. The augmented real-time data streams write directly to **OneLake** inside a target Lakehouse structure, persisted as transactional Delta Parquet files (`brz_stock_ticks`, `brz_news_feed`).
2. The Fabric **Data Factory** invokes a nightly trigger to initiate the transformation layer. This execution activates the native **dbt Job Runtime**.
3. The dbt engine processes the raw tables through SQL data models, building clean, aggregated analytics assets (`fact_market_movements`, `dim_company_profiles`, `fact_prediction_performance`).

### Phase D: Automated MLOps Loop & Presentation

1. A nightly Data Factory schedule executes a Python training script inside the **Synapse Data Science** workspace.
2. The script queries the dbt performance table, measuring variance between previous model forecasts and actual market settlements. If accuracy metrics fall below specified parameters, the notebook loads historical data from OneLake and executes incremental fine-tuning on the time-series model.
3. Hyperparameters, loss metrics, and the final model weight variants are compiled inside the native **MLflow Model Registry**. The updated configuration is marked as active for next-day production deployment.
4. **Power BI** references the transformed tables in **DirectLake Mode**. As new records land in OneLake via the streaming notebooks, the visualizations refresh automatically without executing typical SQL queries.

---

## 5. Environment & Connection Reference Profiles

### Ingestion Script Kafka Connection String

To link Python data compilation scripts to the Fabric Eventstream environment, the streaming connection string utilizes the following configuration payload:

```python
kafka_configuration = {
    'bootstrap.servers': 'WORKSPACE_EVENTSTREAM_ENDPOINT.servicebus.windows.net:9093',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanism': 'PLAIN',
    'sasl.jaas.config': 'org.apache.kafka.common.security.plain.PlainLoginModule required username="$ConnectionString" password="ENDPOINT_SASL_CONNECTION_STRING";'
}

```

### dbt Profile Connection Specification

The local configuration file (`profiles.yml`) required to route development transformation commands into the Fabric Synapse Data Warehouse over port 1433 is defined below:

```yaml
fabric_financial_dw:
  target: dev
  outputs:
    dev:
      type: fabric
      driver: "ODBC Driver 18 for SQL Server"
      host: "your-workspace-sql-endpoint.datawarehouse.fabric.microsoft.com"
      database: "dw_financial_analytics"
      schema: "dbo"
      authentication: CLI
      threads: 4
```

---

