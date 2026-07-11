import os, json, time, threading, websocket
from dotenv import load_dotenv
from confluent_kafka import Producer
from models import AllTickResponse

load_dotenv()
producer = Producer({"bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS")})

def on_message(ws, message):
    raw_json = json.loads(message)
    # Skip standard heartbeat/ping responses from the server
    if "cmd_id" in raw_json and raw_json["cmd_id"] == 22000:
        return
    try:
        validated = AllTickResponse(**raw_json)
        producer.produce(
            topic="raw_alltick_ticks",
            key=validated.data.symbol,
            value=validated.model_dump_json(),
        )
        producer.flush()
        print(f"[AllTick] Sent live crypto tick for {validated.data.symbol} | Price: {validated.data.price}")
    except Exception as e:
        print(f"[-] AllTick Parsing Error: {e}")

def keep_alive(ws):
    """ Sends a heartbeat every 10 seconds so the server connection never drops """
    while True:
        time.sleep(10)
        if ws.sock and ws.sock.connected:
            ws.send(
                json.dumps({"cmd_id": 22000, "seq_id": 1, "trace": "ping", "data": {}})
            )

def on_open(ws):
    print("[*] AllTick Handshake Secure. Subscribing to Crypto and Forex assets...")

    # Unified list capturing Bitcoin, Ethereum, and Euro/US Dollar Forex
    symbols = [
        {"code": "BTCUSDT", "depth_level": 1},
        {"code": "ETHUSDT", "depth_level": 1},
        {"code": "EURUSD", "depth_level": 1}
    ]

    sub_payload = {
        "cmd_id": 22002,
        "seq_id": 2,
        "trace": "sub",
        "data": {"symbol_list": symbols},
    }
    ws.send(json.dumps(sub_payload))
    threading.Thread(target=keep_alive, args=(ws,), daemon=True).start()

if __name__ == "__main__":
    # Target the WebSocket endpoint using your environment variable token key
    url = f"wss://quote.alltick.co/quote-stock-b-ws-api?token={os.getenv('ALLTICK')}"
    ws = websocket.WebSocketApp(url, on_open=on_open, on_message=on_message)
    ws.run_forever()