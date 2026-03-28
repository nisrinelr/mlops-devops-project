import os
import sys
from pyspark.sql.functions import to_timestamp
from pyspark.sql.types import TimestampType

os.environ.pop("SPARK_HOME", None)
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, udf, when, lit, current_timestamp,
    to_json, struct, round as spark_round
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType
)
spark = SparkSession.builder \
    .appName("AirQualitySensorConsumer") \
    .master("local[*]") \
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.2,"   # 👈 3.5.2 to match SPARK_VERSION
            "org.postgresql:postgresql:42.7.3") \
    .config("spark.sql.streaming.checkpointLocation", "/tmp/spark-checkpoints") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

location_schema = StructType([
    StructField("lat", DoubleType()),
    StructField("lon", DoubleType())
])

measurements_schema = StructType([
    StructField("pm2_5",       DoubleType()),
    StructField("pm10",        DoubleType()),
    StructField("no2",         DoubleType()),
    StructField("o3",          DoubleType()),
    StructField("temperature", DoubleType()),
    StructField("humidity",    IntegerType())
])

sensor_schema = StructType([
    StructField("sensor_id",       StringType()),
    StructField("timestamp",       StringType()),
    StructField("location",        location_schema),
    StructField("measurements",    measurements_schema),
    StructField("status",          StringType()),
    StructField("is_true_anomaly", StringType())
])

raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "raw_data") \
    .option("startingOffsets", "latest") \
    .load()

parsed = raw_stream \
    .selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), sensor_schema).alias("data")) \
    .select("data.*")

# Clean & Validate
cleaned = parsed \
    .filter(col("sensor_id").isNotNull()) \
    .filter(col("measurements").isNotNull()) \
    .filter(col("status") == "active") \
    .filter(col("measurements.pm2_5").between(0, 500)) \
    .filter(col("measurements.pm10").between(0, 600)) \
    .filter(col("measurements.temperature").between(-50, 60)) \
    .filter(col("measurements.humidity").between(0, 100))

# Enrich with metrics
def aqi_category(pm2_5):
    if pm2_5 is None:     return "unknown"
    if pm2_5 <= 12:       return "good"
    elif pm2_5 <= 35.4:   return "moderate"
    elif pm2_5 <= 55.4:   return "unhealthy_sensitive"
    elif pm2_5 <= 150:    return "unhealthy"
    else:                 return "hazardous"

aqi_udf = udf(aqi_category, StringType())

enriched = cleaned \
    .withColumn("aqi_category", aqi_udf(col("measurements.pm2_5"))) \
    .withColumn("heat_index",
        spark_round(col("measurements.temperature") * 1.1 +
                    col("measurements.humidity") * 0.05, 2)) \
    .withColumn("processed_at", current_timestamp())

# Anomaly Detection
anomalies = enriched \
    .filter(
        (col("measurements.pm2_5")       > 35.4) |
        (col("measurements.pm10")        > 50.0) |
        (col("measurements.no2")         > 85.0) |
        (col("measurements.o3")          > 100.0) |
        (col("measurements.temperature") > 40.0) |
        (col("measurements.humidity")    > 85)
    ) \
    .withColumn("anomaly_reason",
        when(col("measurements.pm2_5") > 35.4,        lit("HIGH_PM2_5"))
        .when(col("measurements.pm10") > 50.0,        lit("HIGH_PM10"))
        .when(col("measurements.no2") > 85.0,         lit("HIGH_NO2"))
        .when(col("measurements.o3") > 100.0,         lit("HIGH_O3"))
        .when(col("measurements.temperature") > 40.0, lit("EXTREME_TEMP"))
        .otherwise(lit("HIGH_HUMIDITY"))
    ) \
    .withColumn("severity",
        when((col("measurements.pm2_5") > 150) |
             (col("measurements.no2") > 200), lit("critical"))
        .otherwise(lit("warning"))
    )

# Write processed data to Kafka
processed_kafka = enriched \
    .select(
        col("sensor_id").alias("key"),
        to_json(struct("sensor_id", "timestamp", "measurements",
                       "aqi_category", "heat_index", "processed_at",
                       "is_true_anomaly")).alias("value")
    ).writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "processed_data") \
    .option("checkpointLocation", "/tmp/spark-checkpoints/processed-kafka") \
    .outputMode("append") \
    .start()

# Write anomalies to Kafka
anomalies_kafka = anomalies \
    .select(
        col("sensor_id").alias("key"),
        to_json(struct("sensor_id", "timestamp", "measurements",
                       "anomaly_reason", "severity", "processed_at",
                       "is_true_anomaly")).alias("value")
    ).writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "anomalies") \
    .option("checkpointLocation", "/tmp/spark-checkpoints/anomalies-kafka") \
    .outputMode("append") \
    .start()

# Add this after your existing writeStream blocks in consumer.py

def write_to_postgres(df, epoch_id, table):
    df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/airflow") \
        .option("dbtable", table) \
        .option("user", "airflow") \
        .option("password", "airflow") \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()
    
# Write sensor_readings
enriched_flat = enriched.select(
    col("sensor_id"),
    to_timestamp(col("timestamp")).alias("event_time"),  
    col("location.lat").alias("lat"),
    col("location.lon").alias("lon"),
    col("measurements.pm2_5").alias("pm2_5"),
    col("measurements.pm10").alias("pm10"),
    col("measurements.no2").alias("no2"),
    col("measurements.o3").alias("o3"),
    col("measurements.temperature").alias("temperature"),
    col("measurements.humidity").alias("humidity"),
    col("aqi_category"),
    col("heat_index"),
    col("processed_at"),
    col("is_true_anomaly")
)

enriched_flat.writeStream \
    .foreachBatch(lambda df, eid: write_to_postgres(df, eid, "sensor_readings")) \
    .option("checkpointLocation", "/tmp/spark-checkpoints/pg-readings") \
    .outputMode("append") \
    .start()

# Write sensor_anomalies
anomalies_flat = anomalies.select(
    col("sensor_id"),
    to_timestamp(col("timestamp")).alias("event_time"),  
    col("measurements.pm2_5").alias("pm2_5"),
    col("measurements.pm10").alias("pm10"),
    col("measurements.no2").alias("no2"),
    col("measurements.o3").alias("o3"),
    col("measurements.temperature").alias("temperature"),
    col("measurements.humidity").alias("humidity"),
    col("anomaly_reason"),
    col("severity"),
    col("processed_at"),
    col("is_true_anomaly")
)

anomalies_flat.writeStream \
    .foreachBatch(lambda df, eid: write_to_postgres(df, eid, "sensor_anomalies")) \
    .option("checkpointLocation", "/tmp/spark-checkpoints/pg-anomalies") \
    .outputMode("append") \
    .start()
# Console for debugging
enriched.select("sensor_id", "timestamp",
    col("measurements.pm2_5").alias("pm2_5"),
    col("measurements.temperature").alias("temp"),
    "aqi_category", "is_true_anomaly") \
    .writeStream.format("console") \
    .option("truncate", False) \
    .outputMode("append").start()

print("✅ Spark consumer running. Processing and writing to Kafka...")
spark.streams.awaitAnyTermination()