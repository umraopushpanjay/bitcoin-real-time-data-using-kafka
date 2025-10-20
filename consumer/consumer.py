import json
import time
from datetime import datetime
from collections import defaultdict
from kafka import KafkaConsumer
import psycopg2

# Kafka configuration
KAFKA_TOPIC = "btc_price"
KAFKA_SERVER = "localhost:9092"

# PostgreSQL configuration
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
    """
    Insert or update aggregated Bitcoin price data for a 5-minute window.
    
    This function uses PostgreSQL's ON CONFLICT clause to handle duplicate 5-minute
    windows gracefully. If a window already exists, it merges the new data:
    - Adds event counts
    - Recalculates weighted average
    - Updates min/max values
    
    Args:
        conn: Active PostgreSQL connection
        five_min_start (datetime): Start of the 5-minute window
        event_count (int): Number of events in this batch
        avg_value (float): Average price in this batch
        min_value (float): Minimum price in this batch
        max_value (float): Maximum price in this batch
"""
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


def insert_pipeline_metrics(conn, events_count, lag_seconds, data_size_kb):
    """
    Insert pipeline performance metrics for monitoring and alerting.
    
    These metrics are used by Grafana dashboards to monitor pipeline health:
    - Throughput (events processed per interval)
    - Processing lag (how old is the data being processed)
    - Data volume (KB processed per interval)
    
    Args:
        conn: Active PostgreSQL connection
        events_count (int): Number of events processed in this interval
        lag_seconds (float): Time difference between event timestamp and processing time
        data_size_kb (float): Approximate data volume in kilobytes
"""
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO pipeline_metrics (metric_timestamp, events_processed, processing_lag_seconds, data_size_kb)
            VALUES (NOW(), %s, %s, %s)
        """, (events_count, lag_seconds, data_size_kb))
        conn.commit()


def make_five_min_start(dt):
    """
    Round a datetime down to the nearest 5-minute interval.
    This creates consistent 5-minute windows for aggregation.
    Examples:
        10:07:32 -> 10:05:00
        10:13:45 -> 10:10:00
    Args:
        dt (datetime): Input timestamp
        
    Returns:
        datetime: Timestamp rounded down to 5-minute boundary
"""
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
        total_events_in_interval = 0
        last_event_time = None
        
        for msg in consumer:
            event = msg.value
            ts = datetime.fromisoformat(event['timestamp'])
            five_min_start = make_five_min_start(ts)
            buffer[five_min_start].append(event['price'])
            
            total_events_in_interval += 1
            last_event_time = ts  
            
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
                
                # Calculate and insert pipeline metrics
                lag = (datetime.utcnow() - last_event_time).total_seconds() if last_event_time else 0
                data_size = total_events_in_interval * 0.1  # Approx 100 bytes per event = 0.1 KB
                insert_pipeline_metrics(conn, total_events_in_interval, lag, data_size)
                print(f"📊 Metrics: {total_events_in_interval} events | lag {lag:.1f}s | {data_size:.2f} KB")
                
                # Reset counter
                total_events_in_interval = 0  
                last_flush = time.time()
                
    except KeyboardInterrupt:
        print("\nShutting down consumer.")
    finally:
        conn.close()
        consumer.close()