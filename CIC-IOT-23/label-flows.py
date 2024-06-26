import pandas as pd
import os
import numpy as np

loss_directory = "/root/bbdd/logs-zeek/cic-iot-2023-logs/loss-rows"

def process_conn_log(folder_path,binary_label,label,detailed_label):
    conn_log_path = os.path.join(folder_path, "conn_stadistics.log")
    
    # Check if conn.log file exists
    if os.path.exists(conn_log_path):
        folder_name = os.path.basename(folder_path)
        with open(conn_log_path, 'r') as file:
            header_line = file.readlines()[6].strip().split('\t')[1:]
        df = pd.read_csv(conn_log_path, sep='\t', skiprows=8, names=header_line, skipfooter=1, engine='python')
        
        print(folder_name)
        # Check for the file with name folder_name + _loss_rows.csv  
        loss_rows_path = os.path.join(loss_directory, f'{folder_name}_loss_rows.csv')
        if os.path.exists(loss_rows_path):
            print(f"Loss rows file found: {loss_rows_path}")
            df_loss = pd.read_csv(loss_rows_path)
            # Identify rows to be removed
            rows_to_remove = df[df['uid'].isin(df_loss['uid'])]

            # Print the rows that are going to be removed
            print("Rows to be removed:")
            print(rows_to_remove)
            # Remove rows from df where df['uid'] is in df_loss['uid']
            df = df[~df['uid'].isin(df_loss['uid'])]
            df['binary-label']=binary_label
            df['label']=label
            df['detailed-label']=detailed_label
            # Save concatenated data frame to CSV
            output_path = "/root/bbdd/logs-zeek/cic-iot-2023-logs/labeled-csv/"  # Change this to the desired directory path
            csv_filename = os.path.join(output_path, f"{folder_name}_labeled.csv")
            df.to_csv(csv_filename, index=False)
        else:
            print(f"Loss rows file not found for {folder_name}")
        # Once found, open that loss file as csv, look for the uids to remove them in the new df we are going to create
    else:
        print(f"conn.log not found in {folder_path}")

############## GREETH ####################

main_directory = "/root/bbdd/logs-zeek/cic-iot-2023-logs/mirai-greeth"
binary_label = 1
label = "Mirai"
detailed_label="Mirai-greeth"

for folder in os.listdir(main_directory):
    folder_path = os.path.join(main_directory, folder)
    if os.path.isdir(folder_path):
        process_conn_log(folder_path,binary_label,label,detailed_label)

"""

############## GREIP ####################

main_directory = "/root/bbdd/logs-zeek/cic-iot-2023-logs/mirai-greip"
binary_label = 1
label = "Mirai"
detailed_label="Mirai-greip"

for folder in os.listdir(main_directory):
    folder_path = os.path.join(main_directory, folder)
    if os.path.isdir(folder_path):
        process_conn_log(folder_path,binary_label,label,detailed_label)

############# UDP-PLAIN #################
main_directory = "/root/bbdd/logs-zeek/cic-iot-2023-logs/mirai-udpplain"
binary_label = 1
label = "Mirai"
detailed_label="Mirai-udpplain"

for folder in os.listdir(main_directory):
    folder_path = os.path.join(main_directory, folder)
    if os.path.isdir(folder_path):
        process_conn_log(folder_path,binary_label,label,detailed_label)


############# DDOS-HTTP-FLOOD ###################
main_directory = "/root/bbdd/logs-zeek/cic-iot-2023-logs/ddos-http-flood"
binary_label = 1
label = "DDoS"
detailed_label="DDoS-httpflood"

for folder in os.listdir(main_directory):
    folder_path = os.path.join(main_directory, folder)
    if os.path.isdir(folder_path):
        process_conn_log(folder_path,binary_label,label,detailed_label)


############ DDOS PSHACK FLOOD #################
main_directory = "/root/bbdd/logs-zeek/cic-iot-2023-logs/ddos-pshack-flood"
binary_label = 1
label = "DDoS"
detailed_label="DDoS-pshackflood"

for folder in os.listdir(main_directory):
    folder_path = os.path.join(main_directory, folder)
    if os.path.isdir(folder_path):
        process_conn_log(folder_path,binary_label,label,detailed_label)


############ Dictionary Brute Force ###############

main_directory = "/root/bbdd/logs-zeek/cic-iot-2023-logs/DictionaryBruteForce"
binary_label = 1
label = "DictionaryBruteForce"
detailed_label="DictionaryBruteForce"

for folder in os.listdir(main_directory):
    folder_path = os.path.join(main_directory, folder)
    if os.path.isdir(folder_path):
        process_conn_log(folder_path,binary_label,label,detailed_label)

############# Recon OSScan ###############
main_directory = "/root/bbdd/logs-zeek/cic-iot-2023-logs/Recon-OSScan"
binary_label = 1
label = "Scanning"
detailed_label="Scanning-OSScan"

for folder in os.listdir(main_directory):
    folder_path = os.path.join(main_directory, folder)
    if os.path.isdir(folder_path):
        process_conn_log(folder_path,binary_label,label,detailed_label)

############ Recon PortScan ###############
main_directory = "/root/bbdd/logs-zeek/cic-iot-2023-logs/Recon-PortScan"
binary_label = 1
label = "Scanning"
detailed_label="Scanning-PortScan"

for folder in os.listdir(main_directory):
    folder_path = os.path.join(main_directory, folder)
    if os.path.isdir(folder_path):
        process_conn_log(folder_path,binary_label,label,detailed_label)

############## Benign #################
main_directory = "/root/bbdd/logs-zeek/cic-iot-2023-logs/benign"
binary_label = 0
label = "benign"
detailed_label="benign"

for folder in os.listdir(main_directory):
    folder_path = os.path.join(main_directory, folder)
    if os.path.isdir(folder_path):
        process_conn_log(folder_path,binary_label,label,detailed_label)

"""
