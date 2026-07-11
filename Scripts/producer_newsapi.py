import os, time, requests
from dotenv import load_dotenv
from confluent_kafka import Producer
from models import NewsAPIResponse
from datetime import datetime, timedelta

load_dotenv()
producer = Producer(
    {"bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")}
)


def poll():
    one_week_ago = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
    # Expanded search query to target Forex and Crypto topics
    url = f"https://newsapi.org/v2/everything?q=Google+OR+Microsoft+OR+Amazon+OR+Nvidia+OR+Bitcoin+OR+Ethereum&from={one_week_ago}&language=en&apiKey={os.getenv('NEWSAPI')}"
    try:
        res = requests.get(url, timeout=10).json()

        # DataOps Resilience: Catch key issues or limitations early
        if res.get("status") == "error":
            print(f"[-] NewsAPI Alert: {res.get('message')}")
            return

        validated = NewsAPIResponse(**res)
        for article in validated.articles:
            producer.produce(
                topic="raw_newsapi_news",
                key=article.title,
                value=article.model_dump_json(),
            )
        producer.flush()
        print(f"[NewsAPI] Ingested {len(validated.articles)} multi-asset articles.")
    except Exception as e:
        print(f"[-] NewsAPI Error: {e}")


if __name__ == "__main__":
    print("[*] Starting Multi-Asset NewsAPI Poller...")
    while True:
        poll()
        time.sleep(300)  # Poll every 5 minutes
