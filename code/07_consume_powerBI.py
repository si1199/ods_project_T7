import time
import requests
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from functools import reduce

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
    StructField("generation hydro run of river and poundage", DoubleType()),  # Adapted field name
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
    "generation hydro run of river and poundage", "generation hydro water reservoir", "generation nuclear",
    "generation other", "generation other renewable", "generation solar", "generation waste", "generation wind onshore"
]

total_generation_expr = reduce(lambda x, y: x + y, [col(field) for field in generation_columns])
sensor_stream = sensor_stream.withColumn("total_generation", total_generation_expr)


# Write data to Power BI with retry mechanism
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
            # Transform each row into the structure expected by Power BI
            formatted_row = {
                "time": row["time"],
                "total_generation": row["total_generation"],
                "forecast solar day ahead": row["forecast solar day ahead"],
                "forecast wind onshore day ahead": row["forecast wind onshore day ahead"],
                "total load forecast": row["total load forecast"],
                "total load actual": row["total load actual"],
                "price day ahead": row["price day ahead"],
                "price actual": row["price actual"]
            }
            rows.append(formatted_row)
        payload = json.dumps(rows)
        response = requests.post(power_bi_url, headers=headers, data=payload)
        if response.status_code == 200:
            print("Data sent to Power BI successfully.")
            return
        else:
            print(f"Failed to send data to Power BI: {response.status_code} - {response.text}")

    print("Failed to send data to Power BI after maximum retries.")


query = (
    sensor_stream.writeStream
    .foreachBatch(send_to_power_bi)
    .outputMode("append")
    .start()
)

query.awaitTermination()
