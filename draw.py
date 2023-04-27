import matplotlib.pyplot as plt
import pandas as pd

# Load CSV file into a pandas DataFrame
df = pd.read_csv('data.csv', names=['timestamp', 'time_spent'])

# Convert timestamp column to datetime object
df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')

# Set timestamp column as the index
df.set_index('timestamp', inplace=True)

# Plot time spent over real-world time
plt.plot(df.index, df['time_spent'])
plt.xlabel('Timestamp')
plt.ylabel('Time Spent (ms)')
plt.title('Change of Time Spent over Real-World Time')
plt.grid(True)
plt.show()
