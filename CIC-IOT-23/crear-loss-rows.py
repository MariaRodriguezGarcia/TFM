

import pandas as pd
import numpy as np
import os 
header = [
    'ts', 'startTime', 'uid', 'id.orig_h', 'id.orig_p', 'id.resp_h', 'id.resp_p', 'proto', 'service', 'duration',
    'orig_bytes', 'resp_bytes', 'conn_state', 'local_orig', 'local_resp', 'missed_bytes', 'history', 'orig_pkts',
    'orig_ip_bytes', 'resp_pkts', 'resp_ip_bytes', 'tunnel_parents', 'orig_bytes_mean', 'resp_bytes_mean',
    'orig_bytes_std', 'resp_bytes_std', 'orig_bytes_mean_nocero', 'resp_bytes_mean_nocero', 'orig_bytes_std_nocero',
    'resp_bytes_std_nocero', 'orig_bytes_min', 'resp_bytes_min', 'orig_bytes_max', 'resp_bytes_max', 'orig_pkts_nocero',
    'resp_pkts_nocero', 'orig_pkts_cero', 'resp_pkts_cero', 'time_mean', 'time_std', 'time_min', 'time_max',
    'orig_time_mean', 'orig_time_std', 'orig_time_min', 'orig_time_max', 'resp_time_mean', 'resp_time_std', 
    'resp_time_min', 'resp_time_max' ]
def process_conn_log(folder_path,columns = ['uid','missed_bytes','orig_bytes','resp_bytes']):
    conn_log_path = os.path.join(folder_path, "conn_stadistics.log")
    
    # Check if conn.log file exists
    if os.path.exists(conn_log_path):
        folder_name = os.path.basename(folder_path)
        #with open(conn_log_path, 'r') as file:
            #header_line = file.readlines()[6].strip().split('\t')[1:]
        df = pd.read_csv(conn_log_path, sep='\t', skiprows=8, names=header, skipfooter=1, engine='python',usecols=columns)
        df.loc[df['missed_bytes'] == '-', 'missed_bytes'] = np.nan 
        df.loc[df['orig_bytes'] == '-', 'missed_bytes'] = np.nan
        df.loc[df['resp_bytes'] == '-', 'missed_bytes'] = np.nan  

        # Convert remaining NaNs to 0 after substitution
        df['missed_bytes'] = pd.to_numeric(df['missed_bytes'], errors='coerce').fillna(0)
        df['orig_bytes'] = pd.to_numeric(df['orig_bytes'], errors='coerce').fillna(0)
        df['resp_bytes'] = pd.to_numeric(df['resp_bytes'], errors='coerce').fillna(0)

        df['missed_ratio'] = np.where((df['missed_bytes'].isna()) | (df['missed_bytes'] == 0), 0, pd.to_numeric(df['missed_bytes']) / (pd.to_numeric(df['orig_bytes']) + pd.to_numeric(df['resp_bytes'])))
            
        # Filter rows with loss > 0.01 and append to list
        filtered_chunk = df[df['missed_ratio'] > 0.01]
                
        # Filter rows with loss > 0.01 and append to list
        filtered_chunk = df[df['missed_ratio'] > 0.01].copy()  # Make a copy to avoid the warning
        filtered_chunk.loc[:, 'missed_ratio'] = df['missed_ratio']  # Assign values using .loc[]

    print(f"{folder_name}_loss_rows.csv created")
    # Save concatenated data frame to CSV
    output_path = "/root/bbdd/logs-zeek/cic-iot-2023-logs/loss-rows/"  # Change this to the desired directory path
    csv_filename = os.path.join(output_path, f"{folder_name}_loss_rows.csv")
    filtered_chunk.to_csv(csv_filename, index=False)
"""
main_directory = "/root/bbdd/logs-zeek/cic-iot-2023-logs/mirai-udpplain/"

for folder in os.listdir(main_directory):
    folder_path = os.path.join(main_directory, folder)
    if os.path.isdir(folder_path):
        process_conn_log(folder_path)

main_directory = "/root/bbdd/logs-zeek/cic-iot-2023-logs/ddos-http-flood/"

for folder in os.listdir(main_directory):
    folder_path = os.path.join(main_directory, folder)
    if os.path.isdir(folder_path):
        process_conn_log(folder_path)

main_directory = "/root/bbdd/logs-zeek/cic-iot-2023-logs/ddos-pshack-flood"

for folder in os.listdir(main_directory):
    folder_path = os.path.join(main_directory, folder)
    if os.path.isdir(folder_path):
        process_conn_log(folder_path)


main_directory = "/root/bbdd/logs-zeek/cic-iot-2023-logs/Recon-OSScan"

for folder in os.listdir(main_directory):
    folder_path = os.path.join(main_directory, folder)
    if os.path.isdir(folder_path):
        process_conn_log(folder_path)

main_directory = "/root/bbdd/logs-zeek/cic-iot-2023-logs/Recon-PortScan"

for folder in os.listdir(main_directory):
    folder_path = os.path.join(main_directory, folder)
    if os.path.isdir(folder_path):
        process_conn_log(folder_path)

main_directory = "/root/bbdd/logs-zeek/cic-iot-2023-logs/DictionaryBruteForce"

for folder in os.listdir(main_directory):
    folder_path = os.path.join(main_directory, folder)
    if os.path.isdir(folder_path):
        process_conn_log(folder_path)

"""
main_directory = "/root/bbdd/logs-zeek/cic-iot-2023-logs/benign"

for folder in os.listdir(main_directory):
    folder_path = os.path.join(main_directory, folder)
    if os.path.isdir(folder_path):
        process_conn_log(folder_path)

