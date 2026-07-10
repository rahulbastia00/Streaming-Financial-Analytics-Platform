import os, time, requests
from dotenv import load_dotenv
from confluent_kafka import Producer
from models import NewsAPIResponse

load_dotenv()
producer = Producer({'bootstrap.servers': os.getenv("KAFKA_BOOTSTRAP_SERVERS")})

def poll():
    url = f"https://newsapi.org/v2/everything?q=Google+OR+Microsoft+OR+Amazon+OR+Nvidia&apiKey={os.getenv('NEWSAPI_API_KEY')}"
    try:
        res = requests.get(url).json()
        validated = NewsAPIResponse(**res)
        for article in validated.articles:
            producer.produce(topic='raw_newsapi_news', key=article.title, value=article.model_dump_json())
        producer.flush()
        print(f"[NewsAPI] Ingested {len(validated.articles)} articles.")
    except Exception as e: print(f"Error: {e}")

while True:
    poll()
    time.sleep(300)