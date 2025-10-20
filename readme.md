This project implements a complete real-time data processing pipeline that:

- Fetches live Bitcoin prices from Binance API every 10 seconds
- Streams data through Apache Kafka
- Aggregates prices into 5-minute windows (min, max, average)
- Stores aggregated data in PostgreSQL
- Visualizes metrics in Grafana dashboards
- Provides alerting for pipeline health issues

Domain: Cryptocurrency price tracking and analysis
Technologies: Python, Kafka, PostgreSQL, Grafana, Docker



Architecture:


┌─────────────┐      ┌─────────┐      ┌──────────┐      ┌────────────┐
│   Binance   │─────▶│Producer │─────▶│  Kafka   │─────▶│  Consumer  │
│     API     │      │(Python) │      │(btc_price│      │  (Python)  │
└─────────────┘      └─────────┘      │  topic)  │      └──────┬─────┘
                                       └─────────┘             │
                                                                │
                                                                ▼
                     ┌──────────────────────────────────┐  ┌──────────┐
                     │           Grafana                │◀─│PostgreSQL│
                     │  - Bitcoin Price Dashboard       │  │          │
                     │  - Pipeline Performance KPIs     │  │  Tables: │
                     │  - Real-time Monitoring          │  │  • five_ │
                     │  - Alerting System               │  │    min_* │
                     └──────────────────────────────────┘  │  • pipe* │
                                                            └──────────┘


Data Processing

- Real-time streaming from Binance public API
- 5-minute aggregations with min, max, and average calculations
- 30-minute rolling averages for trend analysis
- Irregular interval simulation (10-second fetch intervals)

Monitoring & Observability

- Two comprehensive Grafana dashboards:

    - Bitcoin Price Dashboard: Price trends, ranges, rolling averages
    - Pipeline Performance KPIs: Throughput, lag, data volume


Alerting system for pipeline health:

- High processing lag alert (>60 seconds)
- No data received alert (>3 minutes)


Real-time metrics updated every 5 seconds

Infrastructure

- Fully containerized using Docker Compose
- Local development ready - no cloud dependencies


Prerequisites
Before running this project, ensure you have:

- Docker (version 20.0 or higher)
- Docker Compose (version 2.0 or higher)
- Python 3.8+ (for running producer/consumer locally)
- Internet connection (for fetching Bitcoin prices from Binance API)



Installation & Setup

1. Clone or Extract the Project

cd /path/to/your/workspace
# If you have the project as a zip/tar
tar -xzf realtime-pipeline.tar.gz
cd realtime-pipeline

2. Review Project Structure
Ensure you have all these files:

realtime-pipeline/
│
├── docker-compose.yml          # Container orchestration
│
├── producer/                   # Data source simulator
│   ├── producer.py            # Fetches BTC prices from Binance
│   └── requirements.txt       # Python dependencies
│
├── consumer/                   # Data processor
│   ├── consumer.py            # Consumes, aggregates, stores data
│   └── requirements.txt       # Python dependencies
│
├── infra/
│   └── init_db.sql            # Database schema initialization
│
├── grafana/
│   ├── dashboards/            # Dashboard definitions
│   │   ├── dashboard.yml      # Dashboard provisioning config
│   │   ├── bitcoin-dashboard.json
│   │   └── pipeline-kpi-dashboard.json
│   │
│   ├── datasources/           # Data source configs
│   │   └── datasource.yml     # PostgreSQL connection
│   │
│   └── provisioning/
│       └── alerting/          # Alert rules
│           ├── alerts.yml     # Alert definitions
│           └── contact-points.yml
│
└── README.md                   # This file


3. Start Infrastructure
# Start all Docker containers
docker-compose up -d

# Wait 30 seconds for Kafka to fully initialize
sleep 30

# Verify all containers are running
docker-compose ps

Expected output:
NAME        STATUS          PORTS
grafana     Up 30 seconds   0.0.0.0:3000->3000/tcp
kafka       Up 30 seconds   0.0.0.0:9092->9092/tcp
postgres    Up 30 seconds   0.0.0.0:5432->5432/tcp
zookeeper   Up 30 seconds   0.0.0.0:2181->2181/tcp


4. Install Python Dependencies
# Install producer dependencies
cd producer
pip install -r requirements.txt

# Install consumer dependencies
cd ../consumer
pip install -r requirements.txt


Running the Pipeline
Start the Producer (Terminal 1)
cd producer
python producer.py

Expected output:
Streaming live BTC prices from Binance to Kafka…
Produced: {'symbol': 'BTC/USD', 'price': 43250.75, 'timestamp': '2024-...'}
Produced: {'symbol': 'BTC/USD', 'price': 43251.20, 'timestamp': '2024-...'}


Start the Consumer (Terminal 2)
bashcd consumer
python consumer.py

Expected output:
Consumer started. Processing BTC prices, aggregating every 5 minutes.
Consumed: BTC/USD = $43250.75
Consumed: BTC/USD = $43251.20
✓ Flushed: 2024-10-20 10:00:00 | 3 events | avg $43251.15 | min $43250.75 | max $43252.10
📊 Metrics: 3 events | lag 25.3s | 0.30 KB


Keep Both Running
Leave both terminals running. The pipeline will:

Fetch new Bitcoin prices every 10 seconds
Consume and process them continuously
Flush aggregations every 30 seconds
Update dashboards in real-time

📊 Accessing Dashboards
Grafana
URL: http://localhost:3000
Username: admin
Password: admin
Available Dashboards

Bitcoin Price Dashboard

- Real-time price chart (5-min aggregations)
- Price range visualization (high/low)
- 30-minute rolling average
- Total events processed
- Latest 5-minute price range


Pipeline Performance KPIs

- Pipeline throughput (events per 30s interval)
- Processing lag monitoring
- Current processing rate
- Data volume metrics

Refresh Rate: Dashboards auto-refresh every 5 seconds

🚨 Monitoring & Alerts
Built-in Alerts
The system includes two critical alerts:
1. High Processing Lag Alert

Trigger: Processing lag exceeds 60 seconds
Severity: Warning
Action: Check consumer health and Kafka connection

2. No Data Received Alert

Trigger: No metrics received for 3+ minutes
Severity: Critical
Action: Check if producer/consumer crashed

Viewing Alerts

Go to Grafana → Alerting → Alert rules
Check alert status and history
Configure contact points in grafana/provisioning/alerting/contact-points.yml

PostgreSQL (Optional)
If you want to query the database directly:
# Connect to PostgreSQL
docker exec -it postgres psql -U postgres -d eventsdb

# View aggregated data
SELECT * FROM five_min_bitcoin_event ORDER BY five_min_start DESC LIMIT 10;

# View pipeline metrics
SELECT * FROM pipeline_metrics ORDER BY metric_timestamp DESC LIMIT 10;

# Exit
\q


Kafka (Optional)
Check Kafka topics and messages:
# List topics
docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 --list

# Consume messages from topic (Ctrl+C to stop)
docker exec -it kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic btc_price \
  --from-beginning
