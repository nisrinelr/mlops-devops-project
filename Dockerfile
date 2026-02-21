# Start from the latest Ubuntu LTS
FROM ubuntu:24.04

ENV DEBIAN_FRONTEND=noninteractive

# 1. Install System Dependencies
RUN apt-get update && apt-get install -y \
    openjdk-17-jdk \
    python3-pip \
    python3-dev \
    python3-venv \
    wget \
    curl \
    vim \
    sudo \
    supervisor \
    postgresql \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

# 2. Set Environment Variables
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV SPARK_VERSION=3.5.2
ENV KAFKA_VERSION=4.2.0
ENV SPARK_HOME=/opt/spark
ENV KAFKA_HOME=/opt/kafka
ENV AIRFLOW_HOME=/root/airflow
ENV PATH="/opt/airflow-venv/bin:$SPARK_HOME/bin:$KAFKA_HOME/bin:$PATH"

# Airflow config
ENV AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@localhost/airflow
ENV AIRFLOW__CORE__EXECUTOR=LocalExecutor
ENV AIRFLOW__CORE__AUTH_MANAGER=airflow.providers.fab.auth_manager.fab_auth_manager.FabAuthManager

# 3. Install Apache Spark
RUN wget https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz && \
    mkdir -p $SPARK_HOME && \
    tar -xzf spark-${SPARK_VERSION}-bin-hadoop3.tgz -C $SPARK_HOME --strip-components=1 && \
    rm spark-${SPARK_VERSION}-bin-hadoop3.tgz

# 4. Install Apache Kafka (KRaft mode)
RUN wget https://downloads.apache.org/kafka/${KAFKA_VERSION}/kafka_2.13-${KAFKA_VERSION}.tgz && \
    mkdir -p $KAFKA_HOME && \
    tar -xzf kafka_2.13-${KAFKA_VERSION}.tgz -C $KAFKA_HOME --strip-components=1 && \
    rm kafka_2.13-${KAFKA_VERSION}.tgz

# 5. Install Airflow 3.x in a venv (python3-venv is installed above so this is guaranteed to work)
COPY requirements.txt /tmp/requirements.txt
RUN python3 -m venv /opt/airflow-venv && \
    /opt/airflow-venv/bin/pip install --upgrade pip && \
    /opt/airflow-venv/bin/pip install -r /tmp/requirements.txt && \
    rm /tmp/requirements.txt

# 6. Point Kafka log dirs to a persistent location
RUN mkdir -p /var/kafka-data/broker /var/kafka-data/controller && \
    sed -i 's|log.dirs=.*|log.dirs=/var/kafka-data/broker|' $KAFKA_HOME/config/broker.properties && \
    sed -i 's|log.dirs=.*|log.dirs=/var/kafka-data/controller|' $KAFKA_HOME/config/controller.properties

# 7. Initialize Kafka KRaft Storage
RUN KAFKA_CLUSTER_ID=$(/opt/kafka/bin/kafka-storage.sh random-uuid) && \
    /opt/kafka/bin/kafka-storage.sh format --standalone \
        -t $KAFKA_CLUSTER_ID \
        -c $KAFKA_HOME/config/controller.properties && \
    /opt/kafka/bin/kafka-storage.sh format \
        -t $KAFKA_CLUSTER_ID \
        -c $KAFKA_HOME/config/broker.properties

# 8. Setup PostgreSQL and initialize Airflow DB
# Use pg_ctlcluster directly which is more reliable in Docker than service command
RUN pg_ctlcluster 16 main start && \
    su - postgres -c "psql -c \"CREATE USER airflow WITH PASSWORD 'airflow';\"" && \
    su - postgres -c "psql -c \"CREATE DATABASE airflow OWNER airflow;\"" && \
    /opt/airflow-venv/bin/airflow db migrate && \
    /opt/airflow-venv/bin/airflow users create \
        --username admin \
        --firstname admin \
        --lastname admin \
        --role Admin \
        --email admin@example.com \
        --password admin && \
    pg_ctlcluster 16 main stop

# 9. Copy Supervisor configuration and topic init script
COPY supervisord.conf /etc/supervisor/conf.d/supervisord.conf
COPY init-topics.sh /usr/local/bin/init-topics.sh
RUN chmod +x /usr/local/bin/init-topics.sh

EXPOSE 9092 9093 8080 8081 4040 5432

VOLUME ["/var/kafka-data"]

CMD ["/usr/bin/supervisord"]