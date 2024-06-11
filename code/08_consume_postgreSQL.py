import json
import psycopg2
from kafka import KafkaConsumer

# Kafka parameters
kafka_bootstrap_servers = "localhost:9092"
kafka_topic = "energy-data"

# PostgreSQL parameters
db_host = "localhost"
db_port = "5432"
db_name = "energy-data"
db_user = "postgres"
db_password = "admin"

# Connect to the PostgreSQL database
conn = psycopg2.connect(
    host=db_host,
    port=db_port,
    dbname=db_name,
    user=db_user,
    password=db_password
)


def insert_into_postgresql(data):
    try:
        # Create a cursor object using the connection
        cur = conn.cursor()

        # Example: Insert data into a table named 'energy_data'
        cur.execute("""
            INSERT INTO energy_data (
                time, 
                generation_biomass, 
                generation_fossil_brown_coal_lignite, 
                generation_fossil_gas, 
                generation_fossil_hard_coal, 
                generation_fossil_oil, 
                generation_hydro_pumped_storage_consumption, 
                generation_hydro_run_of_river_and_poundage, 
                generation_hydro_water_reservoir, 
                generation_nuclear, 
                generation_other, 
                generation_other_renewable, 
                generation_solar, 
                generation_waste, 
                generation_wind_onshore, 
                forecast_solar_day_ahead, 
                forecast_wind_onshore_day_ahead, 
                total_load_forecast, 
                total_load_actual, 
                price_day_ahead, 
                price_actual
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                %s
            )
        """, (
            data['time'],
            data['generation_biomass'],
            data['generation_fossil_brown_coal_lignite'],
            data['generation_fossil_gas'],
            data['generation_fossil_hard_coal'],
            data['generation_fossil_oil'],
            data['generation_hydro_pumped_storage_consumption'],
            data['generation_hydro_run_of_river_and_poundage'],
            data['generation_hydro_water_reservoir'],
            data['generation_nuclear'],
            data['generation_other'],
            data['generation_other_renewable'],
            data['generation_solar'],
            data['generation_waste'],
            data['generation_wind_onshore'],
            data['forecast_solar_day_ahead'],
            data['forecast_wind_onshore_day_ahead'],
            data['total_load_forecast'],
            data['total_load_actual'],
            data['price_day_ahead'],
            data['price_actual']
        ))

        # Commit the transaction
        conn.commit()

        print("Data inserted into PostgreSQL successfully!")

    except psycopg2.Error as e:
        # Rollback the transaction in case of error
        conn.rollback()

        print("Error: Failed to insert data into PostgreSQL:", e)

    finally:
        # Close the cursor
        cur.close()

def clear_table():
    try:
        # Create a cursor object using the connection
        cur = conn.cursor()

        # Execute the DELETE query to remove all rows from the table
        cur.execute("DELETE FROM energy_data")

        # Commit the transaction
        conn.commit()

        print("All data deleted from the table successfully!")

    except psycopg2.Error as e:
        # Rollback the transaction in case of error
        conn.rollback()

        print("Error: Failed to delete data from the table:", e)

    finally:
        # Close the cursor
        cur.close()

# Clear the table before inserting new data
clear_table()

# Create a Kafka consumer
consumer = KafkaConsumer(
    kafka_topic,
    bootstrap_servers=kafka_bootstrap_servers,
    auto_offset_reset='earliest',  # Start reading from the earliest message in the topic
    enable_auto_commit=False,  # Disable automatic offset commit
    group_id=None  # Specify a unique group ID for the consumer
)

# Consume messages from the Kafka topic
# Inside the loop where messages are processed
# Inside the loop where messages are processed
for message in consumer:
    try:
        # Convert the Kafka message value (bytes) to a string and strip any leading/trailing whitespace
        message_str = message.value.decode("utf-8").strip()

        # Print the message received from Kafka
        print("Received message:", message_str)

        # Parse the JSON string
        data = json.loads(message_str)

        # Adjust the field names if necessary
        data = {
            "time": data.get("time"),
            "generation_biomass": data.get("generation biomass"),
            "generation_fossil_brown_coal_lignite": data.get("generation fossil brown coal/lignite"),
            "generation_fossil_gas": data.get("generation fossil gas"),
            "generation_fossil_hard_coal": data.get("generation fossil hard coal"),
            "generation_fossil_oil": data.get("generation fossil oil"),
            "generation_hydro_pumped_storage_consumption": data.get("generation hydro pumped storage consumption"),
            "generation_hydro_run_of_river_and_poundage": data.get("generation hydro run-of-river and poundage"),
            "generation_hydro_water_reservoir": data.get("generation hydro water reservoir"),
            "generation_nuclear": data.get("generation nuclear"),
            "generation_other": data.get("generation other"),
            "generation_other_renewable": data.get("generation other renewable"),
            "generation_solar": data.get("generation solar"),
            "generation_waste": data.get("generation waste"),
            "generation_wind_onshore": data.get("generation wind onshore"),
            "forecast_solar_day_ahead": data.get("forecast solar day ahead"),
            "forecast_wind_onshore_day_ahead": data.get("forecast wind onshore day ahead"),
            "total_load_forecast": data.get("total load forecast"),
            "total_load_actual": data.get("total load actual"),
            "price_day_ahead": data.get("price day ahead").replace(",", ".") if data.get("price day ahead") else None,
            "price_actual": data.get("price actual").replace(",", ".") if data.get("price actual") else None
        }

        # Insert the data into PostgreSQL
        insert_into_postgresql(data)

    except Exception as e:
        print("Error processing message:", e)

# Close the Kafka consumer
consumer.close()

# Close the PostgreSQL connection
conn.close()
