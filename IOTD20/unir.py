import os
import glob
import pandas as pd

# Directory where CSV files are located
csv_dir = '/root/bbdd/logs-zeek/iotd20-logs/logs-original-v2/'

# List all CSV files in the directory
csv_files = glob.glob(os.path.join(csv_dir, '*.csv'))

# Initialize an empty list to store DataFrame objects
dfs = []

# Read each CSV file into a DataFrame and append to the list
for csv_file in csv_files:
    df = pd.read_csv(csv_file)
    dfs.append(df)

# Concatenate all DataFrames in the list, ignoring indexes to avoid duplicates
merged_df = pd.concat(dfs, ignore_index=True)

# Write the merged DataFrame to a new CSV file
merged_df.to_csv('merged_file_v2.csv', index=False)

print(f"Merged {len(csv_files)} CSV files into merged_file.csv")
