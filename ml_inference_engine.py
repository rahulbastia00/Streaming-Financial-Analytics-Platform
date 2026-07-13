import os
import uuid
import requests
from datetime import datetime, timedelta
import snowflake.connector
from dotenv import load_dotenv

# Load environmental variables from .env file
load_dotenv()

# ---------------------------------------------------------
# 1. HUGGING FACE CLOUD INFERENCE API SETTINGS
# ---------------------------------------------------------
# Get your free token from https://huggingface.co/settings/tokens
HF_API_TOKEN = os.getenv("HF_API_TOKEN", "hf_your_token_here")
FINBERT_API_URL = "https://router.huggingface.co/hf-inference/models/ProsusAI/finbert"
HEADERS = {"Authorization": f"Bearer {HF_API_TOKEN}"}

def query_finbert_cloud(text_payload):
    """Sends text to Hugging Face Cloud GPUs instead of processing locally."""
    try:
        response = requests.post(FINBERT_API_URL, headers=HEADERS, json={"inputs": text_payload})
        result = response.json()
        
        # Parse the cloud response (usually a list of dicts with label and score)
        if isinstance(result, list) and len(result) > 0:
            top_prediction = result[0][0] # Get the highest confidence label
            label = top_prediction['label']
            score = top_prediction['score']
            
            # Convert to -1.0 to 1.0 scale
            if label == 'positive': return score
            elif label == 'negative': return -score
            else: return 0.0
        return 0.0
    except Exception as e:
        print(f"Cloud API Error: {e}")
        return 0.0

def generate_chronos_forecast(live_price, price_deviation_pct, rolling_volatility):
    """Lightweight mathematical projection simulating Chronos trajectory."""
    drift = (price_deviation_pct / 100.0) * live_price
    volatility_buffer = rolling_volatility * 0.1
    predicted_price = live_price + drift + volatility_buffer
    return float(round(predicted_price, 4))

# ---------------------------------------------------------
# 2. SNOWFLAKE CONNECTIVITY CONFIGURATION
# ---------------------------------------------------------
def get_snowflake_connection():
    return snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse="COMPUTE_WH",
        database="ALPHA_FINANCIAL_DB",
        schema="SILVER"
    )

# ---------------------------------------------------------
# 3. CORE EXECUTION ENGINE
# ---------------------------------------------------------
def run_ml_pipeline():
    conn = get_snowflake_connection()
    cursor = conn.cursor()
    
    try:
        print("--> Fetching feature matrices from Snowflake Gold Layer...")
        # FIX: Changed stock_ticker to asset_ticker to match the table schema
        cursor.execute("SELECT asset_key, asset_ticker, nlp_text_payload FROM fct_nlp_feature_inputs LIMIT 10;")
        news_features = cursor.fetchall()
        
        cursor.execute("SELECT asset_key, stock_ticker, live_price, price_deviation_pct, rolling_volatility FROM fct_market_ml_features;")
        market_dict = {row[1]: (row[0], row[2], row[3], row[4]) for row in cursor.fetchall()}
        
        insert_records = []
        
        print("--> Offloading Inference to Hugging Face Cloud GPUs...")
        for asset_key, ticker, text_payload in news_features:
            if ticker not in market_dict: 
                continue
                
            # 1. Cloud Sentiment Scoring
            sentiment_mapped = query_finbert_cloud(text_payload)
            
            # 2. Time-Series Forecasting
            _, live_price, deviation, volatility = market_dict[ticker]
            forecasted_price = generate_chronos_forecast(live_price, deviation, volatility)
            
            current_time = datetime.now()
            target_window = current_time + timedelta(minutes=5)
            
            insert_records.append((
                str(uuid.uuid4()), asset_key, ticker,
                current_time.strftime('%Y-%m-%d %H:%M:%S'),
                target_window.strftime('%Y-%m-%d %H:%M:%S'),
                float(sentiment_mapped), float(forecasted_price)
            ))
            
        if insert_records:
            print(f"--> Ingesting {len(insert_records)} predictions into Snowflake feedback table...")
            insert_query = """
                INSERT INTO fct_model_predictions 
                (prediction_id, asset_key, stock_ticker, predicted_at, target_window, predicted_sentiment_score, predicted_future_price)
                VALUES (%s, %s, %s, %s, %s, %s, %s);
            """
            cursor.executemany(insert_query, insert_records)
            conn.commit()
            print("--> Pipeline Complete!")
        else:
            print("--> No matching asset features found to process in this cycle.")
            
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    run_ml_pipeline()