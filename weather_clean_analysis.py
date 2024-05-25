import pandas as pd
import os

path = r'C:\Users\Silvia Silva\Desktop\weather_features.csv'

# Read the CSV file
df = pd.read_csv(path)

#print(df.describe())
print(df.dtypes)

# Check for missing values
missing_values = df.isnull().sum()
print("Missing Values:\n", missing_values)

# Remove rows with missing values
df = df.dropna()
# Remove columns with all null values
df = df.dropna(axis=1, how='all')
# Remove columns with all zero values
df = df.dropna(axis=1, how='all')

num_columns_after = df.shape[1]
print("Number of columns after removing all-zero columns:", num_columns_after)

# Convert 'dt_iso' column to datetime and remove timezone information
df['dt_iso'] = pd.to_datetime(df['dt_iso'], utc=True).dt.tz_localize(None)

# Save the cleaned DataFrame to an Excel file
file_name_dest = "weather_clean.xlsx"
directory = r'C:\Users\Silvia Silva\Desktop'
file_path = os.path.join(directory, file_name_dest)
df.to_excel(file_path, index=False)
