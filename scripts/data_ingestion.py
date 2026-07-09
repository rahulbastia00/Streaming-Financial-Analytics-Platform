# pip install pydantic confluent-kafka

from typing import Dict, Any, Type  # Added Dict and Any here
import time
import json
from pydantic import BaseModel, ValidationError, Field
from confluent_kafka import Producer
import os
from dotenv import load_dotenv
import requests

load_dotenv()

# KAFKA PROTOCOL & FABRIC ENDPOINT CONFIGURATION
KAFKA_BOOTSTRAP_SERVERS = os.environ.get('KAFKA_BOOTSTRAP_SERVERS')
KAFKA_SASL_PASSWORD = os.environ.get('KAFKA_SASL_PASSWORD')

kafka_configuration = {
    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
    'security.protocol': 'SASL_SSL',
    'sasl.mechanism': 'PLAIN',
    'sasl.jaas.config': f'org.apache.kafka.common.security.plain.PlainLoginModule required username="$ConnectionString" password="{KAFKA_SASL_PASSWORD}";',
    'client.id': 'fabric-alpha-producer',
    'acks': 'all' 
}

# Creating Kafka Producer
producer = Producer(kafka_configuration)

API_KEYS = {
    "FINNHUB": os.environ.get('FINNHUB'),
    "ALPHA_VANTAGE": os.environ.get("ALPHA_VANTAGE"),
    "ALLTICK": os.environ.get("ALLTICK"),
    "NEWSAPI": os.environ.get("NEWSAPI"),
    "MARKETAUX": os.environ.get("MARKETAUX")
}

# NETWORK DELIVERY CALLBACK LOGGER
def delivery_report(err, msg):
    """ Fired automatically when the Fabric broker returns an execution status """
    if err is not None:
        print(f"[-] Network Delivery Fault: {err}")
    else:
        print(f"[+] Ingestion Success -> Fabric Topic: '{msg.topic()}' | Partition: [{msg.partition()}]")

# LIGHTWEIGHT ROUTING & INGESTION LAYER
def ingest_to_fabric(topic_name: str, raw_payload: Dict[str, Any]):
    """
    Validates basic JSON text integrity to insulate Fabric from Poison Pills,
    then dispatches the raw data directly to its targeted Bronze topic.
    """
    try:
        # Safe JSON Text Serialization Test
        json_string = json.dumps(raw_payload)
        print(f"[*] Shipping raw stream package to target Bronze Topic: '{topic_name}'")

        # Push raw variant directly to Fabric Broker
        producer.produce(
            topic=topic_name,
            value=json_string.encode('utf-8'),
            callback=delivery_report
        )
    except (TypeError, ValueError) as json_err:
        print(f"[-] Poison Pill Dropped! Unparsable data variant: {json_err}")

        error_payload = {
            "error_class": "JSONSerializationViolation",
            "attempted_topic": topic_name,
            "system_timestamp": int(time.time()),
            "partial_string_dump": str(raw_payload)[:500]
        }
        producer.produce(
            topic='ingestion-errors',
            value=json.dumps(error_payload).encode('utf-8'),
            callback=delivery_report
        )
# API FETCH EXECUTORS (REST IMPLEMENTATIONS)
def fetch_finnhub(symbol: str = "MSFT"):
    url = f"https://finnhub.io/api/v1/quote?symbol={symbol}&token={API_KEYS['FINNHUB']}"
    response = requests.get(url)
    if response.status_code == 200:
        # Append meta tags so downstream Silver layer knows the origin
        data = response.json()
        data["_source_api"] = "Finnhub"
        data["_target_symbol"] = symbol
        ingest_to_fabric('esehpnt8zjgv9tpe9oq08x_eh', data) # Main landing topic

def fetch_alpha_vantage(symbol: str = "MSFT"):
    url = f"https://www.alphavantage.co/query?function=GLOBAL_QUOTE&symbol={symbol}&apikey={API_KEYS['ALPHA_VANTAGE']}"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        data["_source_api"] = "AlphaVantage"
        ingest_to_fabric('esehpnt8zjgv9tpe9oq08x_eh', data)

def fetch_alltick(symbol: str = "MSFT"):
    # Simulated structure mapping standard AllTick JSON poll metrics
    url = f"https://api.alltick.co/v1/quote?symbol={symbol}&token={API_KEYS['ALLTICK']}"
    # Adding fallback safety if you haven't filled real tokens yet
    try:
        response = requests.get(url, timeout=5)
        if response.status_code == 200:
            data = response.json()
            data["_source_api"] = "AllTick"
            ingest_to_fabric('esehpnt8zjgv9tpe9oq08x_eh', data)
    except requests.exceptions.RequestException:
        print("[-] AllTick connection timeout. Skipping block instance.")

def fetch_newsapi(query: str = "Microsoft"):
    url = f"https://newsapi.org/v2/everything?q={query}&sortBy=publishedAt&apiKey={API_KEYS['NEWSAPI']}"
    try:
        response = requests.get(url, timeout=5)
        if response.status_code == 200:
            data = response.json()
            data["_source_api"] = "NewsAPI"
            ingest_to_fabric('esehpnt8zjgv9tpe9oq08x_eh', data)
    except requests.exceptions.RequestException:
        print("[-] NewsAPI pipeline offline.")

def fetch_marketaux(symbols: str = "MSFT"):
    url = f"https://api.marketaux.com/v1/news/all?symbols={symbols}&filter_entities=true&api_token={API_KEYS['MARKETAUX']}"
    try:
        response = requests.get(url, timeout=5)
        if response.status_code == 200:
            data = response.json()
            data["_source_api"] = "MarketAux"
            ingest_to_fabric('esehpnt8zjgv9tpe9oq08x_eh', data)
    except requests.exceptions.RequestException:
        print("[-] MarketAux loop disconnected.")

# WORKLOOP EXECUTION
if __name__ == "__main__":
    print("\n[*] Initializing High-Frequency Cross-API Ingestion Gateway...")
    
    # Run a single batch pull across your financial ecosystem
    fetch_finnhub("MSFT")
    fetch_alpha_vantage("MSFT")
    fetch_alltick("MSFT")
    fetch_newsapi("Microsoft")
    fetch_marketaux("MSFT")
    
    # Flush all messages out across active network sockets
    print("[*] Emptying network pipelines...")
    producer.flush()
    print("[*] Transmission batch complete.")