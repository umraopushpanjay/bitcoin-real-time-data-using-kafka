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
        
        for msg in consumer:
            event = msg.value
            ts = datetime.fromisoformat(event['timestamp'])
            five_min_start = make_five_min_start(ts)
            buffer[five_min_start].append(event['price'])
            
            print(f"Consumed: {event['symbol']} = ${event['price']:.2f}")
            
            # Flush every 30 seconds
            if time.time() - last_flush > FLUSH_INTERVAL:
                for five_min, vals in list(buffer.items()):
                    count = len(vals)
                    avg = sum(vals) / count if count else 0
                    min_val = min(vals) if vals else 0
                    max_val = max(vals) if vals else 0
                    upsert_five_min(conn, five_min, count, avg, min_val, max_val)
                    print(f"✓ Flushed: {five_min} | {count} events | avg ${avg:.2f} | min ${min_val:.2f} | max ${max_val:.2f}")
                    del buffer[five_min]
                last_flush = time.time()
                
    except KeyboardInterrupt:
        print("\nShutting down consumer.")
    finally:
        conn.close()
        consumer.close()