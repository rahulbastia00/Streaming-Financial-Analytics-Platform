import os, time, requests
from dotenv import load_dotenv
from confluent_kafka import Producer
from models import MarketAuxResponse

load_dotenv()
producer = Producer({"bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS")})


def poll():
    url = f"https://api.marketaux.com/v1/news/all?symbols=GOOGL,MSFT,AMZN,NVDA&api_token={os.getenv('MARKETAUX')}"
    try:
        res = requests.get(url).json()
        validated = MarketAuxResponse(**res)
        for article in validated.data:
            producer.produce(
                topic="raw_marketaux_news",
                key=article.uuid,
                value=article.model_dump_json(),
            )
        producer.flush()
        print(f"[MarketAux] Ingested {len(validated.data)} articles.")
    except Exception as e:
        print(f"Error: {e}")


while True:
    poll()
    time.sleep(300)  # Wait 5 minutes
