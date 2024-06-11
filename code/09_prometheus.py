import time
import requests
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from functools import reduce
from pyspark.sql.functions import regexp_replace
from prometheus_client import start_http_server, Gauge
from prometheus_client.exposition import generate_latest
from dateutil import parser

# Start Prometheus metrics server
start_http_server(8000)  # Expose metrics on port 8000

# Define Prometheus metrics without timestamp label
total_generation_metric = Gauge('total_generation', 'Total energy generation')
total_load_metric = Gauge('total_load_actual', 'Total energy load')

# Define Kafka parameters
kafka_broker = "localhost:9092"
kafka_topic = "energy-data"

# Define schema for the data
schema = StructType([
    StructField("time", StringType()),
    StructField("generation biomass", DoubleType()),
    StructField("generation fossil brown coal/lignite", DoubleType()),
    StructField("generation fossil gas", DoubleType()),
    StructField("generation fossil hard coal", DoubleType()),
    StructField("generation fossil oil", DoubleType()),
    StructField("generation hydro pumped storage consumption", DoubleType()),
    StructField("generation hydro run-of-river and poundage", DoubleType()),
    StructField("generation hydro water reservoir", DoubleType()),
    StructField("generation nuclear", DoubleType()),
    StructField("generation other", DoubleType()),
    StructField("generation other renewable", DoubleType()),
    StructField("generation solar", DoubleType()),
    StructField("generation waste", DoubleType()),
    StructField("generation wind onshore", DoubleType()),
    StructField("forecast solar day ahead", DoubleType()),
    StructField("forecast wind onshore day ahead", DoubleType()),
    StructField("total load forecast", DoubleType()),
    StructField("total load actual", DoubleType()),
    StructField("price day ahead", StringType()),
    StructField("price actual", StringType())
])

# Power BI parameters
power_bi_url = "https://api.powerbi.com/beta/7a60a629-e61f-430c-9c63-266653b2b899/datasets/6d8c8f31-c3fc-4eba-b808-c304f00f9de6/rows?experience=power-bi&key=L3cRIcduVx2rzwN4NpqmkvKThTI9E1IDTjKDFNCp1lsS4V2Y4k0KS4p4ZDOeUpshuGF1Nl5VY1F5VwYm%2FCR2pA%3D%3D"

# Create SparkSession
spark = (
    SparkSession.builder
    .master("local")
    .appName("Energy Data")
    .config("spark.executor.memory", "1g")
    .config("spark.driver.memory", "1g")
    .config("spark.cores.max", "1")
    .getOrCreate()
)

# Using Kafka as Source for Structured Streaming
sensor_stream = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", kafka_broker)
    .option("startingOffsets", "earliest")
    .option("subscribe", kafka_topic)
    .load()
    .select(from_json(col("value").cast("string"), schema).alias("sensor_data"))
)

# Create the projection
sensor_stream = sensor_stream.select(
    col("sensor_data.*")
)

# Add total_generation column
generation_columns = [
    "generation biomass", "generation fossil brown coal/lignite", "generation fossil gas",
    "generation fossil hard coal", "generation fossil oil", "generation hydro pumped storage consumption",
    "generation hydro run-of-river and poundage", "generation hydro water reservoir", "generation nuclear",
    "generation other", "generation other renewable", "generation solar", "generation waste", "generation wind onshore"
]

total_generation_expr = reduce(lambda x, y: x + y, [col(field) for field in generation_columns])
sensor_stream = sensor_stream.withColumn("total_generation", total_generation_expr)

sensor_stream = sensor_stream.withColumn("price day ahead", regexp_replace(col("price day ahead"), ",", "."))
sensor_stream = sensor_stream.withColumn("price actual", regexp_replace(col("price actual"), ",", "."))

# Add total renewable generation column
renewable_generation_columns = [
    "generation biomass", "generation hydro pumped storage consumption",
    "generation hydro run-of-river and poundage", "generation hydro water reservoir",
    "generation nuclear", "generation other renewable",
    "generation solar", "generation waste", "generation wind onshore"
]

total_renewable_generation_expr = reduce(lambda x, y: x + y, [col(field) for field in renewable_generation_columns])
sensor_stream = sensor_stream.withColumn("total_renewable_generation", total_renewable_generation_expr)

# Add total non-renewable generation column
non_renewable_generation_columns = [
    "generation fossil brown coal/lignite", "generation fossil gas",
    "generation fossil hard coal", "generation fossil oil",
    "generation other"
]

total_non_renewable_generation_expr = reduce(lambda x, y: x + y, [col(field) for field in non_renewable_generation_columns])
sensor_stream = sensor_stream.withColumn("total_non_renewable_generation", total_non_renewable_generation_expr)

# Start HTTP server for Prometheus metrics
def prometheus_metrics():
    return generate_latest()

# Write data to Power BI with retry mechanism
def send_to_power_bi(df, epoch_id):
    data = df.collect()

    headers = {
        "Content-Type": "application/json"
    }

    # Define retry settings
    max_retries = 5
    base_sleep_time = 1  # in seconds

    for retry_count in range(max_retries):
        if retry_count > 0:
            sleep_time = base_sleep_time * (2 ** retry_count)  # exponential backoff
            time.sleep(sleep_time)

        rows = []
        for row in data:
            formatted_row = {
                "time": row["time"],
                "total_generation": row["total_generation"],
                "forecast solar day ahead": row["forecast solar day ahead"],
                "forecast wind onshore day ahead": row["forecast wind onshore day ahead"],
                "total load forecast": row["total load forecast"],
                "total load actual": row["total load actual"],
                "price day ahead": row["price day ahead"],
                "price actual": row["price actual"],
                "generation biomass": row["generation biomass"],
                "generation fossil brown coal/lignite": row["generation fossil brown coal/lignite"],
                "generation fossil gas": row["generation fossil gas"],
                "generation fossil hard coal": row["generation fossil hard coal"],
                "generation fossil oil": row["generation fossil oil"],
                "generation hydro pumped storage consumption": row["generation hydro pumped storage consumption"],
                "generation hydro run-of-river and poundage": row["generation hydro run-of-river and poundage"],
                "generation hydro water reservoir": row["generation hydro water reservoir"],
                "generation nuclear": row["generation nuclear"],
                "generation other": row["generation other"],
                "generation other renewable": row["generation other renewable"],
                "generation solar": row["generation solar"],
                "generation waste": row["generation waste"],
                "total_renewable_generation": row["total_renewable_generation"],
                "total_non_renewable_generation": row["total_non_renewable_generation"],
            }
            rows.append(formatted_row)

            # Update total generation and total load metrics
            total_generation_metric.set(row["total_generation"])
            total_load_metric.set(row["total load actual"] + 100000)

        payload = json.dumps(rows)
        response = requests.post(power_bi_url, headers=headers, data=payload)

        if response.status_code == 200:
            print("Data sent to Power BI successfully.")
            return
        else:
            print(f"Failed to send data to Power BI: {response.status_code} - {response.text}")

    print("Failed to send data to Power BI after maximum retries.")

# Define the Spark Structured Streaming query
query = (
    sensor_stream.writeStream
    .foreachBatch(send_to_power_bi)
    .outputMode("append")
    .start()
)

# Await termination of the Spark Structured Streaming query
query.awaitTermination()
