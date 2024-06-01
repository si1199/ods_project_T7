import pandas as pd
import socket
import time


def generate_shifted_data(df, start_date, periods):
    # Generate a time range with the same frequency as the original data
    date_range = pd.date_range(start=start_date, periods=periods, freq='H', tz='UTC')

    # Create a DataFrame with the shifted data
    shifted_df = df.copy()
    shifted_df.index = date_range
    return shifted_df


def send_data_line_by_line(df, host='127.0.0.1', port=9999):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.connect((host, port))
        # Iterate through the DataFrame
        for index, row in df.iterrows():
            line = f"{index},{','.join(map(str, row.values))}\n"
            # Send the line over the socket connection
            sock.sendall(line.encode('utf-8'))
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

# Send only the shifted data to netcat listener
if __name__ == "__main__":
    send_data_line_by_line(shifted_df, host='127.0.0.1', port=9999)
