import os, time, requests
from dotenv import load_dotenv
from confluent_kafka import Producer
from models import AlphaVantageResponse

load_dotenv()
producer = Producer({"bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS")})


def poll():
    for symbol in ["GOOGL", "MSFT", "AMZN", "NVDA"]:
        url = f"https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol={symbol}&interval=5min&apikey={os.getenv('ALPHA_VANTAGE')}"
        try:
            res = requests.get(url).json()
            validated = AlphaVantageResponse(**res)
            producer.produce(
                topic="raw_alphavantage_candles",
                key=symbol,
                value=validated.model_dump_json(),
            )
            producer.flush()
            print(f"[AlphaVantage] Ingested candles for {symbol}")
            time.sleep(
                15
            )  # Wait 15 seconds between stocks so you don't break Alpha Vantage's free limit (5 calls per minute)
        except Exception as e:
            print(f"Error fetching {symbol}: {e}")


while True:
    poll()
    time.sleep(300)
