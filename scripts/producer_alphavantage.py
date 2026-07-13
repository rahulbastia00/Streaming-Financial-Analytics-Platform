import os, time, requests
from dotenv import load_dotenv
from confluent_kafka import Producer
from models import AlphaVantageResponse

load_dotenv()
producer = Producer({"bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS")})

def run_free_historical_backfill():
    symbols = ["GOOGL", "MSFT", "AMZN", "NVDA"]
    print(f"[*] Markets closed. Initiating free-tier daily historical backfill for: {symbols}")
    
    for symbol in symbols:
        # Changed function to TIME_SERIES_DAILY to leverage the free tier asset history
        url = f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&apikey={os.getenv('ALPHA_VANTAGE')}"
        
        try:
            print(f"[*] Fetching 100-day daily charts from Alpha Vantage for {symbol}...")
            res = requests.get(url, timeout=15).json()
            
            # Resilience Check: Catch structural API blocks or limits
            if "Note" in res or "Information" in res or "Error Message" in res:
                print(f"[-] AlphaVantage API Notice for {symbol}: {res}")
                continue
                
            validated = AlphaVantageResponse(**res)
            
            # Direct transmission into your Kafka broker topic
            producer.produce(
                topic="raw_alphavantage_candles",
                key=symbol,
                value=validated.model_dump_json(),
            )
            producer.flush()
            print(f"[+] AlphaVantage: Successfully saved 100 days of daily history for {symbol}")
            
            # 15-second delay between tickers to comply with the free 5 requests/minute threshold
            time.sleep(15)
            
        except Exception as e:
            print(f"[-] Historical Backfill Failure for {symbol}: {e}")

if __name__ == "__main__":
    run_free_historical_backfill()
    print("[*] Stock historical database populated. Standing by...")
    # Keep process active in background for the master orchestrator script execution
    while True:
        time.sleep(3600)