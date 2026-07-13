import os, time, requests
from dotenv import load_dotenv
from confluent_kafka import Producer
from models import MarketAuxResponse
from datetime import datetime, timedelta

load_dotenv()
producer = Producer(
    {"bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")}
)


def poll():
    one_week_ago = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
    url = f"https://api.marketaux.com/v1/news/all?symbols=GOOGL,MSFT,AMZN,NVDA,CC:BTC,CC:ETH&published_after={one_week_ago}&filter_entities=true&limit=10&api_token={os.getenv('MARKETAUX')}"
    try:
        res = requests.get(url, timeout=10).json()

        # DataOps Resilience: Ignore structural API blocks
        if "error" in res or "message" in res:
            print(
                f"[-] MarketAux API Notice: {res.get('error', {}).get('message', 'Check limits')}"
            )
            return

        validated = MarketAuxResponse(**res)
        for article in validated.data:
            producer.produce(
                topic="raw_marketaux_news",
                key=article.uuid,
                value=article.model_dump_json(),
            )
        producer.flush()
        print(f"[MarketAux] Ingested {len(validated.data)} multi-asset articles.")
    except Exception as e:
        print(f"[-] MarketAux Error: {e}")


if __name__ == "__main__":
    print("[*] Starting Multi-Asset MarketAux Poller...")
    while True:
        poll()
        time.sleep(300)  # Poll every 5 minutes
