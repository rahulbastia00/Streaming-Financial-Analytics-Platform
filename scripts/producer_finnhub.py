import os, json, websocket
from dotenv import load_dotenv
from confluent_kafka import Producer
from models import FinnhubResponse

load_dotenv()
producer = Producer({"bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS")})

def on_message(ws, message):
    raw_json = json.loads(message)
    if raw_json.get("type") == "trade":
        try:
            validated = FinnhubResponse(**raw_json)
            for tick in validated.data:
                producer.produce(
                    topic="raw_finnhub_ticks", key=tick.s, value=tick.model_dump_json()
                )
            producer.flush()
            print(f"[Finnhub] Sent tick for {validated.data[0].s}")
        except Exception as e:
            print(f"Error: {e}")

def on_open(ws):
    print("[*] Finnhub WebSocket open. Listening for live stock trades...")
    for symbol in ["GOOGL", "MSFT", "AMZN", "NVDA"]:
        ws.send(f'{{"type":"subscribe","symbol":"{symbol}"}}')

ws = websocket.WebSocketApp(
    f"wss://ws.finnhub.io?token={os.getenv('FINNHUB')}",
    on_open=on_open,
    on_message=on_message,
)
ws.run_forever()