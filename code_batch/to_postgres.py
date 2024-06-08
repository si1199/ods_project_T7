import pandas as pd
import os
from sqlalchemy import create_engine
import psycopg2

# Change to the correct directory
os.chdir(r'C:\Users\Silvia Silva\Desktop\projeto')

# Read the CSV file into a DataFrame
df = pd.read_csv("energy_clean.csv", header=0,delimiter=';')
df_2 = pd.read_excel("weather_clean.xlsx")

# Ensure that datetime columns are correctly parsed (assuming 'dt_iso' is a datetime column)
#df['time'] = pd.to_datetime(df['time'], utc=True)

# Define the PostgreSQL database credentials
username = 'postgres'
password = 'admin'  # Add your PostgreSQL password if necessary
host = 'localhost'
port = '5433'
database = 'postgres'

# Create the SQLAlchemy engine
engine = create_engine(f'postgresql+psycopg2://{username}:{password}@{host}:{port}/{database}')

# Write the DataFrame to the PostgreSQL table
table_energy = 'energy_data'
df.to_sql(table_energy, engine, if_exists='replace', index=False)
table_weather='weather_data'
df_2.to_sql(table_weather, engine, if_exists='replace', index=False)
print(f"DataFrame written to PostgreSQL table '{table_energy}' & '{table_weather}' successfully.")
