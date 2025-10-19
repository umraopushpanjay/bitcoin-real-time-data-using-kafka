import json
import time
import requests
from datetime import datetime
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

topic = 'btc_price'

def get_realtime_price():
    """Fetch live Bitcoin price from Binance API"""
    url = 'https://api.binance.com/api/v3/ticker/price?symbol=BTCUSDT'
    r = requests.get(url, timeout=5)
    data = r.json()
    price = float(data['price'])
    return {
        "symbol": "BTC/USD",
        "price": price,
        "timestamp": datetime.utcnow().isoformat()
    }

if __name__ == "__main__":
    print("Streaming live BTC prices from Binance to Kafka…")
    while True:
        try:
            payload = get_realtime_price()
            producer.send(topic, payload)
            print("Produced:", payload)
            time.sleep(10)  # Fetch every 10 seconds
        except KeyboardInterrupt:
            break
        except Exception as e:
            print("Error:", e)
            time.sleep(10)