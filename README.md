# MLOps Air Quality Monitoring System

## Project Overview
This project implements a complete MLOps pipeline for real-time air quality monitoring using IoT sensors. The system simulates sensor data, processes it through Kafka and Spark Streaming, stores it in PostgreSQL, and uses Airflow for orchestration and ML model training.

## Architecture
```
IoT Sensors → Kafka (raw_data) → Spark Streaming → PostgreSQL + Kafka (processed_data/anomalies)
                                      ↓
Airflow DAGs → Daily Aggregation → ML Pipeline (Training/Evaluation/Deployment)
```

## Phase 1: Ingestion & ETL Streaming ✅

### Components
- **Sensor Simulation** (`sensors_sim.py`): Generates realistic IoT sensor data with daily patterns and labeled anomalies
- **Kafka Topics**: `raw_data`, `processed_data`, `anomalies`
- **Spark Consumer** (`spark_consumer.py`): Real-time ETL processing
- **PostgreSQL Storage**: `sensor_readings`, `sensor_anomalies`, `sensor_daily_summary`

### Features
- Data validation and cleaning
- AQI category calculation
- Heat index computation
- Rule-based anomaly detection
- Real-time streaming to multiple sinks

## Phase 2: Orchestration with Airflow ✅

### DAGs Implemented

#### 1. ETL Aggregation DAG (`ETL (phase 1 dag)/airflow_sens_dag.py`)
- **Schedule**: Daily at 01:00
- **Tasks**:
  - Create/update database tables
  - Aggregate 24h sensor data
  - Generate daily summaries with statistics

#### 2. ML Pipeline DAG (`phase 2 dags/pipeline.py`)
- **Schedule**: Daily at 01:30
- **Sensors**:
  - `SqlSensor`: Wait for ≥50 new readings in last 24h
  - `SqlSensor`: Wait for daily summary completion
- **ML Tasks**:
  - Extract latest 24h data
  - Data quality validation
  - Train Isolation Forest model
  - Evaluate model performance
  - Conditional deployment based on improvement

#### 3. Backfill DAG (`phase 2 dags/backfill_dag.py`)
- **Trigger**: Manual with date parameters
- **Purpose**: Historical model retraining
- **Features**: Configurable date ranges and contamination rates

## Data Schema

### Sensor Data Structure
```json
{
  "sensor_id": "CAP_001",
  "timestamp": "2024-01-15T14:30:00Z",
  "location": {"lat": 48.8566, "lon": 2.3522},
  "measurements": {
    "pm2_5": 12.5,
    "pm10": 25.3,
    "no2": 40.2,
    "o3": 80.1,
    "temperature": 22.5,
    "humidity": 65
  },
  "status": "active",
  "is_true_anomaly": "True"
}
```

### ML Problems Addressed

1. **Binary Classification**: Anomaly detection using `is_true_anomaly` labels
2. **Time Series Prediction**: Pollution forecasting (H+1)
3. **Clustering**: Sensor profiling by measurement patterns

## Setup Instructions

### Prerequisites
- Docker & Docker Compose
- Python 3.8+
- Java 8+ (for Spark)

### Installation
1. Clone repository
2. Start services: `docker-compose up -d`
3. Initialize Kafka topics: `./init-topics.sh`
4. Run sensor simulation: `python sensors_sim.py`
5. Start Spark consumer: `python spark_consumer.py`

### Airflow Setup
- Access Airflow UI at `http://localhost:8080`
- Enable DAGs in Airflow interface
- Monitor pipeline execution

## Key Features

### Realistic Data Generation
- Daily pollution/temperature cycles using sine waves
- Natural noise and correlations between measurements
- Labeled anomalies for supervised learning

### Streaming Architecture
- Fault-tolerant Kafka messaging
- Spark Structured Streaming for real-time processing
- Multi-sink data distribution

### MLOps Pipeline
- Automated daily model training
- Model evaluation and A/B testing simulation
- Conditional deployment based on performance

### Monitoring & Quality
- Data quality validation
- Anomaly detection rules
- Comprehensive logging and error handling

## Technologies Used
- **Data Ingestion**: Kafka, Python
- **Processing**: Apache Spark, PySpark
- **Storage**: PostgreSQL
- **Orchestration**: Apache Airflow
- **ML**: scikit-learn (Isolation Forest)
- **Containerization**: Docker, Docker Compose

## Next Steps (Phase 3)
- Implement time series forecasting models
- Add model versioning with MLflow
- Create real-time inference API
- Add monitoring dashboards
- Implement automated retraining triggers
