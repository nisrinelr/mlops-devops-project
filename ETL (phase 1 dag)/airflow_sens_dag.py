from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator as PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "miae",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# ── SQL: Create tables if not exist ──────────────────────────────────────────
CREATE_TABLES_SQL = """
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
    processed_at TIMESTAMP DEFAULT NOW()
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
    processed_at TIMESTAMP DEFAULT NOW()
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
"""

# ── SQL: Rolling 24h Aggregation ─────────────────────────────────────────────
DAILY_AGGREGATION_SQL = """
INSERT INTO sensor_daily_summary (
    summary_date, sensor_id,
    avg_pm2_5, max_pm2_5, min_pm2_5,
    avg_pm10, max_pm10,
    avg_no2, max_no2,
    avg_o3,
    avg_temperature, max_temperature,
    avg_humidity,
    total_readings, anomaly_count, dominant_aqi
)

WITH last_24h_readings AS (
    SELECT *
    FROM sensor_readings
    WHERE event_time >= NOW() - INTERVAL '24 hours'
      AND event_time < NOW()
),

aggregated_readings AS (
    SELECT
        DATE(NOW()) AS summary_date,
        sensor_id,

        AVG(pm2_5)  AS avg_pm2_5,
        MAX(pm2_5)  AS max_pm2_5,
        MIN(pm2_5)  AS min_pm2_5,

        AVG(pm10)   AS avg_pm10,
        MAX(pm10)   AS max_pm10,

        AVG(no2)    AS avg_no2,
        MAX(no2)    AS max_no2,

        AVG(o3)     AS avg_o3,

        AVG(temperature) AS avg_temperature,
        MAX(temperature) AS max_temperature,

        AVG(humidity) AS avg_humidity,
        COUNT(*) AS total_readings

    FROM last_24h_readings
    GROUP BY sensor_id
),

anomaly_counts AS (
    SELECT
        sensor_id,
        COUNT(*) AS anomaly_count
    FROM sensor_anomalies
    WHERE event_time >= NOW() - INTERVAL '24 hours'
      AND event_time < NOW()
    GROUP BY sensor_id
),

dominant_aqi AS (
    SELECT DISTINCT ON (sensor_id)
        sensor_id,
        aqi_category
    FROM last_24h_readings
    GROUP BY sensor_id, aqi_category
    ORDER BY sensor_id, COUNT(*) DESC
)

SELECT
    r.summary_date,
    r.sensor_id,

    ROUND(r.avg_pm2_5::numeric, 2),
    ROUND(r.max_pm2_5::numeric, 2),
    ROUND(r.min_pm2_5::numeric, 2),

    ROUND(r.avg_pm10::numeric, 2),
    ROUND(r.max_pm10::numeric, 2),

    ROUND(r.avg_no2::numeric, 2),
    ROUND(r.max_no2::numeric, 2),

    ROUND(r.avg_o3::numeric, 2),

    ROUND(r.avg_temperature::numeric, 2),
    ROUND(r.max_temperature::numeric, 2),

    ROUND(r.avg_humidity::numeric, 1),

    r.total_readings,
    COALESCE(a.anomaly_count, 0),
    d.aqi_category

FROM aggregated_readings r
LEFT JOIN anomaly_counts a
    ON r.sensor_id = a.sensor_id
LEFT JOIN dominant_aqi d
    ON r.sensor_id = d.sensor_id

ON CONFLICT (summary_date, sensor_id) DO UPDATE SET
    avg_pm2_5       = EXCLUDED.avg_pm2_5,
    max_pm2_5       = EXCLUDED.max_pm2_5,
    min_pm2_5       = EXCLUDED.min_pm2_5,
    avg_pm10        = EXCLUDED.avg_pm10,
    max_pm10        = EXCLUDED.max_pm10,
    avg_no2         = EXCLUDED.avg_no2,
    max_no2         = EXCLUDED.max_no2,
    avg_o3          = EXCLUDED.avg_o3,
    avg_temperature = EXCLUDED.avg_temperature,
    max_temperature = EXCLUDED.max_temperature,
    avg_humidity    = EXCLUDED.avg_humidity,
    total_readings  = EXCLUDED.total_readings,
    anomaly_count   = EXCLUDED.anomaly_count,
    dominant_aqi    = EXCLUDED.dominant_aqi,
    created_at      = NOW();
"""

# ── Logging ───────────────────────────────────────────────────────────────────
def log_summary(**context):
    hook = PostgresHook(conn_id="postgres_default")
    records = hook.get_records("""
        SELECT sensor_id, summary_date, total_readings, anomaly_count, dominant_aqi
        FROM sensor_daily_summary
        WHERE summary_date = DATE(NOW())
        ORDER BY sensor_id
    """)
    print("=== Rolling 24h Summary ===")
    for r in records:
        print(f"Sensor: {r[0]} | Date: {r[1]} | Readings: {r[2]} | Anomalies: {r[3]} | AQI: {r[4]}")
    print(f"Total sensors processed: {len(records)}")

# ── DAG Definition ────────────────────────────────────────────────────────────
with DAG(
    dag_id="sensor_daily_batch",
    default_args=default_args,
    description="Rolling 24h aggregation of sensor readings",
    schedule="0 1 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["dataops", "sensors", "batch"],
) as dag:

    create_tables = PostgresOperator(
        task_id="create_tables",
        conn_id="postgres_default",
        sql=CREATE_TABLES_SQL,
    )

    daily_aggregation = PostgresOperator(
        task_id="daily_aggregation",
        conn_id="postgres_default",
        sql=DAILY_AGGREGATION_SQL,
    )

    log_results = PythonOperator(
        task_id="log_results",
        python_callable=log_summary,
    )

    create_tables >> daily_aggregation >> log_results
