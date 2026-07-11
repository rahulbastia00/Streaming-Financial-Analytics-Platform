import json
import websocket

API_KEY = "1af484397a687ef124afbb6ee5a73b1c"


def on_message(ws, message):
    print("\n" + "=" * 80)
    print("RAW MESSAGE RECEIVED:")
    try:
        data = json.loads(message)
        print(json.dumps(data, indent=2))
    except Exception:
        print(message)


def on_error(ws, error):
    print(f"\nERROR: {error}")


def on_close(ws, close_status_code, close_msg):
    print(f"\nConnection closed: {close_status_code} - {close_msg}")


def on_open(ws):
    print("Connected to AllTick!")

    payload = {
        "cmd_id": 22002,
        "seq_id": 1,
        "trace": "subscribe",
        "data": {
            "symbol_list": [
                {"code": "BTCUSDT", "depth_level": 1},
                {"code": "ETHUSDT", "depth_level": 1},
                {"code": "EURUSD", "depth_level": 1},
            ]
        },
    }

    print("\nSending subscription:")
    print(json.dumps(payload, indent=2))
    ws.send(json.dumps(payload))


if __name__ == "__main__":
    url = f"wss://quote.alltick.co/quote-b-ws-api?token={API_KEY}"

    ws = websocket.WebSocketApp(
        url,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close,
    )

    ws.run_forever()