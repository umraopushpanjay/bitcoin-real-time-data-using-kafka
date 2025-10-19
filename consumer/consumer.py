import json
import time
from datetime import datetime
from collections import defaultdict
from kafka import KafkaConsumer
import psycopg2

KAFKA_TOPIC = "btc_price"
KAFKA_SERVER = "localhost:9092"

DB_CONFIG = {
    "dbname": "eventsdb",
    "user": "postgres",
    "password": "postgres",
    "host": "localhost",
    "port": 5432
}

def db_conn():
    return psycopg2.connect(**DB_CONFIG)

def upsert_five_min(conn, five_min_start, event_count, avg_value, min_value, max_value):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO five_min_bitcoin_event (five_min_start, event_count, avg_value, min_value, max_value)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (five_min_start) DO UPDATE
            SET event_count = five_min_bitcoin_event.event_count + EXCLUDED.event_count,
                avg_value = (five_min_bitcoin_event.avg_value * five_min_bitcoin_event.event_count + 
                            EXCLUDED.avg_value * EXCLUDED.event_count) / 
                            (five_min_bitcoin_event.event_count + EXCLUDED.event_count),
                min_value = LEAST(five_min_bitcoin_event.min_value, EXCLUDED.min_value),
                max_value = GREATEST(five_min_bitcoin_event.max_value, EXCLUDED.max_value);
        """, (five_min_start, event_count, avg_value, min_value, max_value))
        conn.commit()

# NEW: Function to insert pipeline metrics
def insert_pipeline_metrics(conn, events_count, lag_seconds, data_size_kb):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO pipeline_metrics (metric_timestamp, events_processed, processing_lag_seconds, data_size_kb)
            VALUES (NOW(), %s, %s, %s)
        """, (events_count, lag_seconds, data_size_kb))
        conn.commit()

def make_five_min_start(dt):
    minutes = (dt.minute // 5) * 5
    return dt.replace(minute=minutes, second=0, microsecond=0)

if __name__ == '__main__':
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=[KAFKA_SERVER],
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='consumer-group-1'
    )

    buffer = defaultdict(list)
    conn = db_conn()
    
    print("Consumer started. Processing BTC prices, aggregating every 5 minutes.")

    try:
        last_flush = time.time()
        FLUSH_INTERVAL = 30
        total_events_in_interval = 0  # NEW: Track events per interval
        last_event_time = None  # NEW: Track lag
        
        for msg in consumer:
            event = msg.value
            time.sleep(30)
            ts = datetime.fromisoformat(event['timestamp'])
            five_min_start = make_five_min_start(ts)
            buffer[five_min_start].append(event['price'])
            
            total_events_in_interval += 1  # NEW
            last_event_time = ts  # NEW
            
            print(f"Consumed: {event['symbol']} = ${event['price']:.2f}")
            
            # Flush every 30 seconds
            if time.time() - last_flush > FLUSH_INTERVAL:
                total_flushed = 0
                for five_min, vals in list(buffer.items()):
                    count = len(vals)
                    avg = sum(vals) / count if count else 0
                    min_val = min(vals) if vals else 0
                    max_val = max(vals) if vals else 0
                    upsert_five_min(conn, five_min, count, avg, min_val, max_val)
                    print(f"✓ Flushed: {five_min} | {count} events | avg ${avg:.2f} | min ${min_val:.2f} | max ${max_val:.2f}")
                    total_flushed += count
                    del buffer[five_min]
                
                # NEW: Calculate and insert pipeline metrics
                lag = (datetime.utcnow() - last_event_time).total_seconds() if last_event_time else 0
                data_size = total_events_in_interval * 0.1  # Approx 100 bytes per event = 0.1 KB
                insert_pipeline_metrics(conn, total_events_in_interval, lag, data_size)
                print(f"📊 Metrics: {total_events_in_interval} events | lag {lag:.1f}s | {data_size:.2f} KB")
                
                total_events_in_interval = 0  # Reset counter
                last_flush = time.time()
                
    except KeyboardInterrupt:
        print("\nShutting down consumer.")
    finally:
        conn.close()
        consumer.close()