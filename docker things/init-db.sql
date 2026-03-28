CREATE TABLE IF NOT EXISTS sensor_readings (
    id SERIAL PRIMARY KEY,
    sensor_id VARCHAR(50),
    event_time TIMESTAMP,
    lat DOUBLE PRECISION,
    lon DOUBLE PRECISION,
    pm2_5 DOUBLE PRECISION,
    pm10 DOUBLE PRECISION,
    no2 DOUBLE PRECISION,
    o3 DOUBLE PRECISION,
    temperature DOUBLE PRECISION,
    humidity INTEGER,
    aqi_category VARCHAR(30),
    heat_index DOUBLE PRECISION,
    processed_at TIMESTAMP DEFAULT NOW(),
    is_true_anomaly VARCHAR(10)
);

CREATE TABLE IF NOT EXISTS sensor_anomalies (
    id SERIAL PRIMARY KEY,
    sensor_id VARCHAR(50),
    event_time TIMESTAMP,
    pm2_5 DOUBLE PRECISION,
    pm10 DOUBLE PRECISION,
    no2 DOUBLE PRECISION,
    o3 DOUBLE PRECISION,
    temperature DOUBLE PRECISION,
    humidity INTEGER,
    anomaly_reason VARCHAR(50),
    severity VARCHAR(20),
    processed_at TIMESTAMP DEFAULT NOW(),
    is_true_anomaly VARCHAR(10)
);

CREATE TABLE IF NOT EXISTS sensor_daily_summary (
    id SERIAL PRIMARY KEY,
    summary_date DATE,
    sensor_id VARCHAR(50),
    avg_pm2_5 DOUBLE PRECISION,
    max_pm2_5 DOUBLE PRECISION,
    min_pm2_5 DOUBLE PRECISION,
    avg_pm10 DOUBLE PRECISION,
    max_pm10 DOUBLE PRECISION,
    avg_no2 DOUBLE PRECISION,
    max_no2 DOUBLE PRECISION,
    avg_o3 DOUBLE PRECISION,
    avg_temperature DOUBLE PRECISION,
    max_temperature DOUBLE PRECISION,
    avg_humidity DOUBLE PRECISION,
    total_readings INTEGER,
    anomaly_count INTEGER,
    dominant_aqi VARCHAR(30),
    created_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(summary_date, sensor_id)
);

-- Grant airflow user full access
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO airflow;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO airflow;

-- Also apply to any future tables created
ALTER DEFAULT PRIVILEGES IN SCHEMA public
    GRANT ALL PRIVILEGES ON TABLES TO airflow;
ALTER DEFAULT PRIVILEGES IN SCHEMA public
    GRANT ALL PRIVILEGES ON SEQUENCES TO airflow;