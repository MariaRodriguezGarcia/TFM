main_directory = "/root/bbdd/logs-zeek/iot-23-logs/labeled-csv/"
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import os

def process_conn_log(folder_name, thresholds=[(0, 0.05), (0.05, 0.1), (0.1, 0.2), (0.2, 1)], columns=["uid", "missed_bytes", "orig_bytes", "resp_bytes", "id.orig_h", "id.orig_p", "id.resp_h", "id.resp_p", "proto", 'binary-label']):
    conn_log_path = os.path.join(main_directory, folder_name)
    
    # Check if conn.log file exists
    if os.path.exists(conn_log_path):
       
        missed_ratio_values = []
        binary_labels = [] 
        for i, chunk in enumerate(pd.read_csv(conn_log_path, usecols=columns, chunksize=50000)):
            # Calculate missed_bytes ratio
            chunk['missed_ratio'] = np.where((chunk['missed_bytes'].isna()) | (chunk['missed_bytes'] == 0), 0, pd.to_numeric(chunk['missed_bytes']) / (pd.to_numeric(chunk['orig_bytes']) + pd.to_numeric(chunk['resp_bytes'])))
            if chunk['missed_ratio'].isnull().values.any():
                print("Warning: NaN value detected in missed_ratio column!")

            # Append missed ratio values and chunk number
            missed_ratio_values.extend(chunk['missed_ratio'])
            binary_labels.extend(chunk['binary-label'])
        

        # Define color based on binary-label
        colors = np.where(np.array(binary_labels) == 0, 'green', 'red')
        sorted_values = sorted(missed_ratio_values, reverse=True)

        # Plot missed ratio values against chunk numbers
        plt.figure(figsize=(10, 6))
        plt.scatter(list(range(1, len(sorted_values) + 1)), sorted_values, c=colors, s=10, alpha=0.5)
        plt.title(f"Missed Bytes Ratio vs Chunk Number - {folder_name}")
        plt.xlabel("Chunk Number")
        plt.ylabel("Missed Bytes Ratio")
        plt.savefig(f"{folder_name}_missed_ratio_vs_chunk.png")
        plt.show()

json_files = [f for f in os.listdir(main_directory) if f.startswith("json")]
for json_file in json_files:
        process_conn_log(json_file)
