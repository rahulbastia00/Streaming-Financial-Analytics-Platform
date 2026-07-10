import os, json
from dotenv import load_dotenv
from confluent_kafka import Consumer
import snowflake.connector

load_dotenv()

# Establish Secure Connection to Snowflake Cloud Warehouse
conn = snowflake.connector.connect(
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    warehouse="FINANCIAL_COMPUTE_WH",
    database="ALPHA_FINANCIAL_DB",
    schema="BRONZE"
)
cursor = conn.cursor()

# Configure Local Kafka Connection Settings
consumer = Consumer({
    'bootstrap.servers': os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
    'group.id': 'snowflake-bridge-group',
    'auto.offset.reset': 'earliest'
})

# Map Each Kafka Topic to Its Matching Snowflake Table
topic_map = {
    'raw_finnhub_ticks': 'bronze_finnhub_ticks',
    'raw_alltick_ticks': 'bronze_alltick_ticks',
    'raw_marketaux_news': 'bronze_marketaux_news',
    'raw_newsapi_news': 'bronze_newsapi_news',
    'raw_alphavantage_candles': 'bronze_alphavantage_candles'
}

consumer.subscribe(list(topic_map.keys()))
print("[*] Snowflake Consumer Bridge Active. Streaming data to the cloud...")

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None or msg.error(): continue

        # Pull out the topic name and the raw payload message string
        topic = msg.topic()
        table_name = topic_map.get(topic)
        payload = msg.value().decode('utf-8')

        if table_name:
            # Safely swallow raw stringified JSON directly into the VARIANT column
            sql = f"INSERT INTO {table_name} (raw_payload) SELECT PARSE_JSON(%s)"
            cursor.execute(sql, (payload,))
            print(f"[+] Synced record from Kafka topic '{topic}' into Snowflake table '{table_name}'")

except KeyboardInterrupt:
    print("[!] Stopping Snowflake Bridge...")
finally:
    consumer.close(); cursor.close(); conn.close()