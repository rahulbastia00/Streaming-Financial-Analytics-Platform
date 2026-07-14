# Streaming Financial Analytics Platform: AWS Cloud-Infrastructure Deployment Guide

This guide details the process of migrating a local financial data streaming pipeline to a production-grade cloud infrastructure on Amazon Web Services (AWS).

By deploying your Python orchestration script to an Amazon EC2 (Elastic Compute Cloud) instance and routing data through Amazon MSK (Managed Streaming for Apache Kafka) to Snowflake, you create an enterprise-grade real-time ingestion engine.

---

# High-Level System Architecture

Before executing, it is critical to visualize how the data flows privately and securely across AWS resources:

```text
                                +-------------------------------------------------------------+
                                |                      AWS VPC (Gated Property)               |
                                |                                                             |
+----------------------+        |   +-----------------------+   +-----------------------+    |
| Financial APIs       |        |   | Public Subnet (Rooms) |   | Private Subnet (Vault)|    |
| (Finnhub, AllTick,   |===HTTPS===>| [ EC2 Instance ]      |   | [ MSK Kafka Cluster ] |    |
| MarketAux, NewsAPI,  |        |   | - Python runtime      |   | - kafka.t3.small      |    |
| AlphaVantage)        |        |   | - run_all.py scripts  |   | - Port 9092 (Plain)   |    |
+----------------------+        |   +-----------+-----------+   +-----------+-----------+    |
                                |               |                           ^                |
                                |               |=== (Private Net over TCP) ================ |
                                |               |                           |                |
                                |               |         Locked by Security Group           |
                                +---------------|--------------------------------------------+
                                                |
                                                | (JDBC/SSL Connection)
                                                v
                                       +------------------+
                                       |    Snowflake     |
                                       |  Cloud Database  |
                                       +------------------+
```

---

# Foundational Concepts

### VPC (Virtual Private Cloud)

Think of this as your private, fenced-off estate in the AWS cloud. No one can enter or exit unless explicitly allowed via an internet gateway or network firewall.

### Subnets

Rooms inside your private estate.

- **Public Subnets:** Rooms with windows. Resources in these subnets (such as your EC2 server) have an IP address exposed to the internet, allowing you to connect via SSH and download dependencies.
- **Private Subnets:** Vaults inside the house. Highly sensitive infrastructure (like your MSK Kafka broker instances) is placed here, completely blocked from direct public internet exposure.

### Security Groups

Dynamic virtual firewalls guarding your servers. By default, AWS locks down all incoming ports on a resource. You must explicitly configure "rules" to allow incoming traffic.

---

# Step-by-Step Walkthrough

# Step 1: Spin up the EC2 Instance (Compute Node)

To host and run the python orchestrator background tasks:

1. Navigate to the EC2 Dashboard in the AWS console and select **Launch Instance**.
2. Choose **Ubuntu 22.04 LTS** or **24.04 LTS (64-bit x86 architecture)** as your machine image.
3. Choose an instance type. A **t3.medium (2 vCPUs, 4 GiB Memory)** is ideal for running the background threads and multiple subprocesses of the 6 running tasks simultaneously.

### Configure network settings:

- Ensure it is placed in your Default VPC (or targeted VPC).
- Assign a Public IP (so you can connect to it).
- Create or select a `.pem` Key Pair to allow SSH access, then click **Launch**.

---

# Step 2: Provision a Developer-Friendly, Cost-Effective Amazon MSK Cluster

By default, standard AWS MSK setups are engineered for massive enterprises, resulting in high configurations and pricing. We intentionally modify these default configurations to create a lightweight, plaintext-accessible broker cluster.

1. Navigate to the Amazon MSK Console and click **Create cluster**.
2. Select **Custom create** (**do NOT use Quick create** as it will deploy expensive instance sizes by default).

## Cluster Settings

- **Cluster type:** Select **Provisioned** (**do NOT select Serverless; see Section 4 for details**).
- **Apache Kafka Version:** Choose the latest stable release (e.g., **3.9.x with KRaft Metadata mode**).

## Brokers

- Broker size: Change from `kafka.m7g.large` (which costs `$0.20+/hour` per broker) to `kafka.t3.small` (which costs `~$0.04/hour`). This saves hundreds of dollars a month during development.
- Brokers per zone: `1` (usually deployed across 2 or 3 Availability Zones for minimal redundant nodes).

## Networking

- Select the same VPC ID as your EC2 Instance (e.g., `vpc-04fa422912acf24a4`).
- Select 2 or 3 of your private/public subnets as requested.

## Security / Access Control Methods

Check the box next to **Unauthenticated access** (this allows connection without generating intricate IAM AWS access signatures, mirroring local development behavior).

## Encryption (Data in Transit)

**Crucial:** Check the box next to **Plaintext** under client-broker communication. If Plaintext is disabled, AWS locks your cluster to encrypted port `9094`, breaking standard local consumer code.

Review and click **Create cluster**. It takes approximately **15 to 20 minutes** to provision in the background.

---

# Step 3: Bridge the Security Group Firewall (The "Jacket" Rule)

Even if your EC2 instance and your MSK cluster live in the exact same VPC, AWS blocks communication between them until they are explicitly linked via Security Groups.

Rather than creating a detached firewall rule, we edit the Default Security Group assigned to your MSK cluster:

1. Go to your EC2 Instances dashboard, select your Ubuntu instance, and click the **Security** tab.
2. Note the Security Group ID (e.g., `sg-0f2dd0906ec12e509` or standard `launch-wizard-*`).
3. Go to the **Security Groups** page under **Network & Security**.
4. Locate the Security Group assigned to your MSK Cluster (often named `default` with ID `sg-09621603eb757ce3b`).
5. Select this default group, click **Inbound Rules**, and select **Edit inbound rules**.

### Add a new rule:

- **Type:** Custom TCP
- **Port Range:** `9092` (Plaintext Kafka Port)
- **Source:** Select Custom and paste the EC2 Security Group ID you copied in Step 2.

Save Rules.

Your EC2 virtual machine now has a private network tunnel straight into the MSK brokers!

---

# Step 4: Configure your EC2 Virtual Environment

Connect to your EC2 instance via SSH and run system-level updates to build your project environment.

### Update package repositories

```bash
sudo apt update && sudo apt upgrade -y
```

### Install essential compilation dependencies and C-libraries for Confluent Kafka

```bash
sudo apt install -y git python3-pip python3-venv librdkafka-dev build-essential
```

### Clone your project repository (No containerization configuration required on server)

```bash
git clone <repository-url>
cd <repository-folder>
```

### Setup python environment

```bash
python3 -m venv venv
source venv/bin/activate
```

### Install requirements

```bash
pip install --upgrade pip
pip install -r requirements.txt
```

---

# Step 5: Configure Cloud Environment Variables

You must update your local `.env` configuration file to let python know where to stream data over the cloud.

1. Go to the Amazon MSK console and select your newly active cluster.
2. Click the **View client information** button in the top right.
3. Locate the cluster's Plaintext connection strings and copy the entire bootstrap server string.

It should look like this:

```text
b-1.kafkaclusters.abc.c2.kafka.ap-south-1.amazonaws.com:9092,
b-2.kafkaclusters.abc.c2.kafka.ap-south-1.amazonaws.com:9092
```

In your EC2 terminal, open or create your `.env` file:

```bash
nano .env
```

Define your active variables.

```env
# API Keys
FINNHUB=d976...
ALPHA_VANTAGE=047J...
ALLTICK=1af4...
NEWSAPI=715e...
MARKETAUX=HaHg...

# Managed Kafka Config (AWS MSK Bootstrap String)
KAFKA_BOOTSTRAP_SERVERS=b-1.kafkaclusters.abc.c2.kafka.ap-south-1.amazonaws.com:9092,b-2.kafkaclusters.abc.c2.kafka.ap-south-1.amazonaws.com:9092

# Snowflake Credentials
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_ACCOUNT=your_account_identifier
```

Save and close the file:

- `Ctrl + O`
- `Enter`
- `Ctrl + X`

---

# Step 6: Launch and Verify the Live Pipelines

Run your orchestrator command within your activated virtual environment on the EC2 machine:

```bash
python3 run_all.py
```

Successful execution logs look like:

```text
[] Launching the complete Financial Data Stack...
[] Snowflake Consumer Bridge Active. Streaming data to the cloud...
[] Finnhub WebSocket open. Listening for live stock trades...

%6|1784015611.260|GETSUBSCRIPTIONS|rdkafka#consumer-1|
[thrd:main]: Telemetry client instance id changed from
AAAAAAAAAAAAAAAAAAAAAA to KRG1FRZISOuH2muEnwMmnA

%6|1784015611.432|GETSUBSCRIPTIONS|rdkafka#producer-1|
[thrd:main]: Telemetry client instance id changed from
AAAAAAAAAAAAAAAAAAAAAA to Yga2J45lReGcflYIHXGSHA

[] Starting Multi-Asset MarketAux Poller...
[] Starting Multi-Asset NewsAPI Poller...
[] Markets closed. Initiating free-tier daily historical backfill...
[+] Success! All 6 channels are streaming live in the background.
```

### What do `%6|GETSUBSCRIPTIONS|... Telemetry client instance id changed` logs mean?

In `librdkafka (confluent-kafka engine)`, a `%6` log is informational (**INFO**).

It serves as definitive proof that your EC2 script successfully negotiated connectivity with the MSK brokers, established an active connection handshake, and received a registered, unique client instance id.

---

# Real-World Debugging Traps Tackled

## Trap 1: The "Serverless" MSK IAM Authorization Trap

### The Issue

Attempting to use AWS MSK "Serverless" requires you to authenticate using AWS Identity and Access Management (IAM) permissions.

Standard open-source Kafka clients (like `confluent_kafka` in python) do not support basic SASL/unauthenticated configurations over Serverless, resulting in immediate script failures unless you wrap your code with AWS-specific signature helpers.

### The Solution

Provisioned standard clusters permit the configuration of Unauthenticated Access over internal VPC lines, allowing us to keep clean, performant, and portable code scripts.

---

## Trap 2: The Default Broker Size Pricing Trap

### The Issue

AWS automatically pre-selects `kafka.m7g.large` brokers when setting up custom clusters.

Since MSK deploys multi-broker environments for zone redundancy, running three of these nodes continuously charges `~$0.60/hour`, adding up to over `$430 per month` for an idle stack.

### The Solution

Intentionally downscaling the broker instances to `kafka.t3.small` during the provisioning wizard limits billing significantly while offering more than enough compute memory to run all six pipeline tasks.

---

## Trap 3: The "Closed Plaintext Port" Trap

### The Issue

By default, AWS disables plaintext transmissions to secure client connections.

If you attempt to connect over standard port `9092` with Plaintext disabled, your scripts will experience connection timeout errors.

### The Solution

We manually went into Step 3 (Security Configuration) during MSK creation and enabled Plaintext explicitly to match standard port configurations.

---

## Trap 4: The Security Group "Jacket" Analogy Misalignment

### The Issue

When trying to resolve port access, it is intuitive to create a brand-new security group (e.g., `kafka-rule`) to govern traffic.

However, unless you explicitly edit the MSK cluster configuration to wear/use this new security group, AWS ignores your rules.

### The Solution

We edited the cluster's active assigned default Security Group and appended a custom TCP rule targeting port `9092` sourcing from the EC2 instance's group ID directly.

---

# Verification: Landing Data in Snowflake

Once your EC2 python terminal prints the active ingestion logs, open your Snowflake Console and execute a quick data query to witness live ingestion:

```sql
SELECT *
FROM YOUR_DATABASE.YOUR_SCHEMA.RAW_FINNHUB_TICKS
ORDER BY INGESTED_AT DESC
LIMIT 10;
```

You should see streaming market ticks, news articles, and historical stock calculations rendering with real-time timestamps in Snowflake.

**Your Cloud-Native Streaming Platform is officially operational!**
