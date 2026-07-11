import os, time, requests
from dotenv import load_dotenv
from confluent_kafka import Producer
from models import AlphaVantageResponse

load_dotenv()
producer = Producer({"bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS")})

def run_weekend_backfill():
    symbols = ["GOOGL", "MSFT", "AMZN", "NVDA"]
    print(f"[*] Markets closed. Initiating deep historical backfill for: {symbols}")
    
    for symbol in symbols:
        # Crucial Shift: Added outputsize=full to extract deep historical 5-min candles
        url = f"https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol={symbol}&interval=5min&outputsize=compact&apikey={os.getenv('ALPHA_VANTAGE')}"
        
        try:
            print(f"[*] Fetching historical payload from Alpha Vantage for {symbol}...")
            res = requests.get(url, timeout=15).json()
            
            # Resilience Check: Catch API rate limiting notes or information blocks
            if "Information" in res or "Note" in res:
                print(f"[-] AlphaVantage Cooldown Triggered: {res.get('Information') or res.get('Note')}")
                return
                
            validated = AlphaVantageResponse(**res)
            
            # Commit the entire historical block cleanly to Kafka
            producer.produce(
                topic="raw_alphavantage_candles",
                key=symbol,
                value=validated.model_dump_json(),
            )
            producer.flush()
            print(f"[+] AlphaVantage Simulator: Successfully saved full history for {symbol}")
            
            # Sleep 15 seconds to respect free-tier limitations (5 calls per minute max)
            time.sleep(15)
            
        except Exception as e:
            print(f"[-] Backfill Error for {symbol}: {e}")

if __name__ == "__main__":
    run_weekend_backfill()
    print("[*] Historical stock backfill completed. Entering standby mode...")
    # Sleep indefinitely so the process stays alive within the orchestrator script
    while True:
        time.sleep(3600)