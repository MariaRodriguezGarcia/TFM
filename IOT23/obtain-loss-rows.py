import os
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

main_directory = "/root/bbdd/logs-zeek/iot-23-logs/labeled-csv/"
import pandas as pd

def process_conn_log(folder_name,columns = ['uid','missed_bytes','orig_bytes','resp_bytes']):
    conn_log_path = os.path.join(main_directory, folder_name)
    loss_rows_df = []  # Initialize a list to store data frames for chunks with loss > 0.01
    
    # Check if conn.log file exists
    if os.path.exists(conn_log_path):
        for chunk in pd.read_csv(conn_log_path,usecols=columns, chunksize=50000):
            # Calculate missed_bytes ratio
            chunk['missed_ratio'] = np.where((chunk['missed_bytes'].isna()) | (chunk['missed_bytes'] == 0), 0, pd.to_numeric(chunk['missed_bytes']) / (pd.to_numeric(chunk['orig_bytes']) + pd.to_numeric(chunk['resp_bytes'])))
            
            # Filter rows with loss > 0.01 and append to list
            filtered_chunk = chunk[chunk['missed_ratio'] > 0.01].copy()
            filtered_chunk.loc[:, 'missed_ratio'] = chunk['missed_ratio']
            loss_rows_df.append(filtered_chunk)

    # Concatenate data frames in the list
    loss_rows_df = pd.concat(loss_rows_df)  
    # Save concatenated data frame to CSV
    output_path = "/root/bbdd/logs-zeek/iot-23-logs/loss-rows/"  # Change this to the desired directory path
    csv_filename = os.path.join(output_path, f"{folder_name}_loss_rows.csv")
    loss_rows_df.to_csv(csv_filename, index=False)
json_files = [f for f in os.listdir(main_directory) if f.startswith("json")]
for json_file in json_files:
        process_conn_log(json_file)
