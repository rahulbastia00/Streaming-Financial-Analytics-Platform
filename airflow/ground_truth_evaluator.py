import os
import snowflake.connector
from dotenv import load_dotenv

# Ensure the non-interactive SSH tunnel loads environment keys perfectly
load_dotenv()

def evaluate_predictions():
    print("--> Connecting to Snowflake Warehouse to evaluate closed prediction windows...")
    
    conn = snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse="COMPUTE_WH",
        database="ALPHA_FINANCIAL_DB",
        schema="SILVER"
    )
    cursor = conn.cursor()

    # 1. Identify closed windows: Predictions awaiting actual validation values
    find_pending_sql = """
        SELECT prediction_id, stock_ticker, target_window, predicted_future_price, predicted_sentiment_score
        FROM fct_model_predictions
        WHERE actual_price IS NULL 
          AND target_window <= current_timestamp();
    """
    cursor.execute(find_pending_sql)
    pending_predictions = cursor.fetchall()
    
    print(f"--> Found {len(pending_predictions)} closed windows awaiting evaluation.")

    for pred in pending_predictions:
        pred_id, ticker, target_time, predicted_price, sentiment = pred
        
        # 2. Schema-Corrected Query Matrix: Using TRADE_PRICE and EVENT_TIMESTAMP
        lookup_actual_sql = """
            SELECT TRADE_PRICE 
            FROM fct_realtime_market_stream 
            WHERE STOCK_TICKER = %s 
              AND abs(timestampdiff(second, EVENT_TIMESTAMP, %s)) <= 10
            ORDER BY EVENT_TIMESTAMP DESC
            LIMIT 1;
        """
        cursor.execute(lookup_actual_sql, (ticker, target_time))
        actual_price_record = cursor.fetchone()
        
        if actual_price_record:
            actual_price = float(actual_price_record[0])
            
            # 3. Compute Real-Time Performance Analytics
            variance = actual_price - float(predicted_price)
            
            # Direct accuracy check (True if prediction lands within a 1% error margin)
            is_correct = abs(variance) / actual_price <= 0.01 
            
            # 4. Save metrics into your historical target log table
            update_sql = """
                UPDATE fct_model_predictions
                SET actual_price = %s,
                    price_error_variance = %s,
                    is_prediction_correct = %s,
                    dw_inserted_at = current_timestamp()
                WHERE prediction_id = %s;
            """
            cursor.execute(update_sql, (actual_price, variance, is_correct, pred_id))
            print(f"[+] Evaluated {ticker}: Pred={predicted_price}, Actual={actual_price}, Error={round(variance, 4)}")
            
    conn.commit()
    cursor.close()
    conn.close()
    print("--> Feedback loop complete. Data layers optimized for retraining matrices.")

if __name__ == "__main__":
    evaluate_predictions()