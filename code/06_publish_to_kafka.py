import pandas as pd
import socket
import time
import json
from kafka import KafkaProducer, KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import KafkaError


def generate_shifted_data(df, start_date, periods):
    # Generate a time range with the same frequency as the original data
    date_range = pd.date_range(start=start_date, periods=periods, freq='H', tz='UTC')

    # Create a DataFrame with the shifted data
    shifted_df = df.copy()
    shifted_df.index = date_range
    return shifted_df


def connect_to_kafka():
    # Create the Kafka producer object
    return KafkaProducer(
        bootstrap_servers=[kafka_broker],
        value_serializer=lambda m: json.dumps(m).encode("ascii"),
    )


def topic_exists(broker, new_topic):
    # create a consumer object
    consumer = KafkaConsumer(bootstrap_servers=[broker])

    # for each topic in the list of available topics
    # check if it matches the provided topic
    for topic in consumer.topics():
        if new_topic == topic:
            print(f"{new_topic} exists")
            return True
    return False


def create_topic(broker, topic, admin_client):
    # create a NewTopic object, given the name,
    # number of partitions and replication factor
    new_topic = [NewTopic(name=topic, num_partitions=2, replication_factor=1)]
    # if it does not exist, create it
    if not topic_exists(broker, topic):
        print(f"creating {topic}")
        admin_client.create_topics(new_topics=new_topic, validate_only=False)


def send_data_to_kafka(df, producer, topic):
    for index, row in df.iterrows():
        # Convert each row to a dictionary
        message = row.to_dict()
        message['time'] = index.isoformat()  # Add the time index as a string

        # Publish to Kafka
        future = producer.send(topic, value=message)
        try:
            record_metadata = future.get(timeout=10)
            print(f"Topic {record_metadata.topic}")
            print(f"Partition {record_metadata.partition}")
            print(f"Published offset {record_metadata.offset}")

        except KafkaError as e:
            print(f"Something went wrong: {e}")

        # Wait for one second before sending the next line
        time.sleep(1)


# Load the original dataset
csv_path = 'data/energy_clean 1.csv'  # Update this path accordingly
df = pd.read_csv(csv_path, sep = ";")

# Convert the 'time' column to datetime
df['time'] = pd.to_datetime(df['time'], format='%Y-%m-%d %H:%M:%S', utc=True)

# Set the 'time' column as the index
df.set_index('time', inplace=True)

# Define the original data range and the new data range
original_start = df.index.min()
original_end = df.index.max()
new_start = original_end + pd.Timedelta(hours=1)

# Calculate the number of periods (rows) in the original DataFrame
periods = len(df)

# Generate shifted data
shifted_df = generate_shifted_data(df, new_start, periods)

# Kafka configurations
kafka_broker = "localhost:9092"
kafka_topic = "energy-data"

# Connect to Kafka
producer = connect_to_kafka()

# Create Kafka topic if it doesn't exist
admin_client = KafkaAdminClient(bootstrap_servers=[kafka_broker])
create_topic(kafka_broker, kafka_topic, admin_client)

# Send the shifted data to the Kafka topic
send_data_to_kafka(shifted_df, producer, kafka_topic)

# Be sure the messages are all sent
producer.flush()
