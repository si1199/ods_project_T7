import os
import pandas as pd
import matplotlib.pyplot as plt
from scipy.stats import norm
import numpy as np


def clean_csv(path):
    # Read the CSV file
    df = pd.read_csv(path)
    print(df.describe())
    df["time"] = pd.to_datetime(df["time"], format='%Y-%m-%d %H:%M:%S%z', utc=True)
    print(df.dtypes)
    df = df.dropna(axis=1, how='all')
    print(df.head(10))
    num_columns = df.shape[1]
    print("Number of columns:", num_columns)

    # Initially 35064 rows
    # Remove all rows that are empty except the first one
    index_time = df.columns[0]
    df = df.dropna(subset=df.columns[1:], how='all')
    num_rows = df.shape[0]
    print("Number of rows before cleaning:", num_rows)

    # Remove rows empty by column
    for i in df.columns:
        df = df.dropna(subset=[i])
    num_rows = df.shape[0]
    print("Number of rows after cleaning:", num_rows)

    # Remove columns with all zero values
    df = df.loc[:, (df != 0).any(axis=0)]
    num_columns_after = df.shape[1]
    print("Number of columns after removing all-zero columns:", num_columns_after)
    return df

def standardize(df):
    # Exclude the time column
    df_ex_time = df.iloc[:, 1:]
    df_s = (df_ex_time - df_ex_time.mean()) / df_ex_time.std()
    df_s.insert(0, 'time', df['time'])  # Insert the time column back at the start
    return df_s

def analysis(df):
    # Exclude the time column for analysis
    df_ex_time = df.iloc[:, 1:]
    df_ex_time.hist(figsize=(10, 8))
    plt.tight_layout()
    plt.show()

    df_ex_time.boxplot(figsize=(14, 8), whis=3.0)
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()

# Define the path and clean the CSV
path = r'C:\Users\Silvia Silva\Desktop\energy_dataset.csv'
df_clean = clean_csv(path)

# Standardize the DataFrame
df_standard = standardize(df_clean)

# Convert datetimes to timezone-unaware
df_clean['time'] = df_clean['time'].dt.tz_localize(None)

# Perform distribution analysis
analysis(df_clean)

# Define the destination file name and directory
file_name_dest = "energy_clean.xlsx"
directory = r'C:\Users\Silvia Silva\Desktop'
file_path = os.path.join(directory, file_name_dest)

# Ensure the directory exists
if not os.path.exists(directory):
    os.makedirs(directory)

try:
    df_clean.to_excel(file_path, index=False)
    print(f'DataFrame written to {file_path}')
except Exception as e:
    print(f"Error saving DataFrame to Excel: {e}")
