CREATE TABLE IF NOT EXISTS five_min_bitcoin_event (
    five_min_start TIMESTAMP PRIMARY KEY,
    event_count INT NOT NULL,
    avg_value DOUBLE PRECISION NOT NULL,
    min_value DOUBLE PRECISION,
    max_value DOUBLE PRECISION
);