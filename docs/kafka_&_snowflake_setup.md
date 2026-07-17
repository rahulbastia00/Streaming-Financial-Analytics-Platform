# Streaming Financial Analytics Platform: AWS MSK and Snowflake Setup Guide

This guide walks through deploying a real-time financial data streaming platform on AWS using:

- Amazon EC2
- Amazon MSK (Managed Apache Kafka)
- Snowflake Cloud Data Warehouse
- Python Consumer Bridge

The final architecture streams financial market data from multiple APIs into Kafka and persists the raw JSON payloads inside Snowflake for downstream dbt transformations.

---

# High-Level System Architecture

```text
+----------------------+
| Financial APIs       |
|----------------------|
| Finnhub              |
| AllTick              |
| MarketAux            |
| NewsAPI              |
| AlphaVantage         |
+----------+-----------+
           |
           | HTTPS
           v
+----------------------+
| Amazon EC2           |
|----------------------|
| Python Runtime       |
| run_all.py           |
| Kafka Producers      |
| Snowflake Consumer   |
+----------+-----------+
           |
           | Private VPC Network
           v
+----------------------+
| Amazon MSK           |
|----------------------|
| Kafka Topics         |
| raw_finnhub_ticks    |
| raw_alltick_ticks    |
| raw_marketaux_news   |
| raw_newsapi_news     |
| raw_alphavantage_    |
| candles              |
+----------+-----------+
           |
           | Consumer Bridge
           v
+----------------------+
| Snowflake            |
|----------------------|
| Database             |
| ALPHA_FINANCIAL_DB   |
| Schema               |
| BRONZE               |
+----------+-----------+
           |
           v
+----------------------+
| dbt                  |
|----------------------|
| Silver Layer         |
| Gold Layer           |
+----------------------+
```

---

# Foundational Concepts

## VPC (Virtual Private Cloud)

A private network inside AWS where all resources communicate securely.

## Public Subnet

Contains internet-accessible resources such as the EC2 instance.

## Private Subnet

Contains sensitive infrastructure such as Kafka brokers.

## Security Groups

Virtual firewalls that control inbound and outbound traffic.

---

# Step 1: Launch the EC2 Instance

1. Navigate to the EC2 Dashboard.
2. Click **Launch Instance**.
3. Select:

- Ubuntu 22.04 LTS or 24.04 LTS
- Instance Type: `t3.medium`

Configure:

- Assign Public IP
- Select or create a `.pem` key pair.

Launch the instance.

---

# Step 2: Create the Amazon MSK Cluster

Navigate to:

```text
AWS Console → Amazon MSK → Create Cluster
```

Select:

- Custom Create
- Provisioned Cluster
- Kafka Version: Latest Stable Release

---

## Broker Configuration

| Setting                     | Value                  |
| --------------------------- | ---------------------- |
| Broker Size                 | kafka.t3.small         |
| Brokers Per Zone            | 1                      |
| Authentication              | Unauthenticated Access |
| Client-Broker Communication | Plaintext              |

---

## Networking

- Select the same VPC as the EC2 instance.
- Choose the required subnets.

Create the cluster.

Provisioning typically takes 15–20 minutes.

---

# Step 3: Configure Security Groups

Find the Security Group attached to:

- EC2 Instance
- MSK Cluster

Add an inbound rule to the MSK Security Group:

| Setting | Value                 |
| ------- | --------------------- |
| Type    | Custom TCP            |
| Port    | 9092                  |
| Source  | EC2 Security Group ID |

Save the rule.

---

# Step 4: Configure the EC2 Environment

SSH into the EC2 machine:

```bash
ssh -i key.pem ubuntu@<public-ip>
```

Update packages:

```bash
sudo apt update && sudo apt upgrade -y
```

Install dependencies:

```bash
sudo apt install -y git python3-pip python3-venv librdkafka-dev build-essential
```

Clone the repository:

```bash
git clone <repository-url>
cd <repository-name>
```

Create the virtual environment:

```bash
python3 -m venv venv
source venv/bin/activate
```

Install requirements:

```bash
pip install --upgrade pip
pip install -r requirements.txt
```

---

# Step 5: Configure Environment Variables

Create the `.env` file:

```bash
nano .env
```

Add:

```env
# API Keys
FINNHUB=
ALPHA_VANTAGE=
ALLTICK=
NEWSAPI=
MARKETAUX=

# Kafka
KAFKA_BOOTSTRAP_SERVERS=

# Snowflake
SNOWFLAKE_USER=
SNOWFLAKE_PASSWORD=
SNOWFLAKE_ACCOUNT=
```

Save:

```text
Ctrl + O
Enter
Ctrl + X
```

---

# Step 6: Configure Snowflake

## Snowflake Resources

| Resource  | Value                |
| --------- | -------------------- |
| Warehouse | FINANCIAL_COMPUTE_WH |
| Database  | ALPHA_FINANCIAL_DB   |
| Schema    | BRONZE               |

---

## Python Connection

```python
conn = snowflake.connector.connect(
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    warehouse="FINANCIAL_COMPUTE_WH",
    database="ALPHA_FINANCIAL_DB",
    schema="BRONZE"
)
```

---

# Step 7: Kafka to Snowflake Consumer Bridge

The consumer continuously reads messages from Kafka and inserts them into Snowflake.

## Consumer Configuration

```python
consumer = Consumer({
    'bootstrap.servers': os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
    'group.id': 'snowflake-bridge-group',
    'auto.offset.reset': 'earliest'
})
```

---

## Topic Mapping

```python
topic_map = {
    'raw_finnhub_ticks': 'bronze_finnhub_ticks',
    'raw_alltick_ticks': 'bronze_alltick_ticks',
    'raw_marketaux_news': 'bronze_marketaux_news',
    'raw_newsapi_news': 'bronze_newsapi_news',
    'raw_alphavantage_candles': 'bronze_alphavantage_candles'
}
```

---

## Data Ingestion Logic

```python
sql = f"""
INSERT INTO {table_name} (raw_payload)
SELECT PARSE_JSON(%s)
"""
cursor.execute(sql, (payload,))
```

The JSON payload is stored inside the `RAW_PAYLOAD` column as a Snowflake `VARIANT`.

---

# Why Use VARIANT?

- Supports semi-structured JSON.
- Preserves original API responses.
- Handles nested objects.
- Simplifies downstream dbt transformations.

---

# Step 8: Launch the Pipeline

```bash
python3 run_all.py
```

Expected logs:

```text
[*] Snowflake Consumer Bridge Active.
[*] Finnhub WebSocket Open.
[*] Starting Multi-Asset MarketAux Poller.
[*] Starting Multi-Asset NewsAPI Poller.
[+] Success! All channels are streaming.
```

---

# Verify Data in Snowflake

```sql
SELECT *
FROM ALPHA_FINANCIAL_DB.BRONZE.BRONZE_FINNHUB_TICKS
LIMIT 10;
```

Inspect raw JSON:

```sql
SELECT RAW_PAYLOAD
FROM ALPHA_FINANCIAL_DB.BRONZE.BRONZE_FINNHUB_TICKS
LIMIT 5;
```

---

# Final Architecture

```text
Financial APIs
        ↓
Amazon MSK (Kafka Topics)
        ↓
Python Consumer Bridge
        ↓
Snowflake Bronze Layer
        ↓
dbt Silver Layer
        ↓
dbt Gold Layer
```

---

# Platform Status

✅ Amazon EC2 Configured

✅ Amazon MSK Cluster Active

✅ Kafka Topics Streaming

✅ Snowflake Consumer Bridge Running

✅ Raw Data Landing in Bronze Tables

✅ Ready for dbt Transformations
