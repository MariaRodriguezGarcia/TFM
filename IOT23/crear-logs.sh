#!/bin/bash

# Path to the source folder containing CTU* folders
source_folder="/root/bbdd/iot-23/"

# Path to the destination folder where logs will be saved
destination_folder="/root/bbdd/logs-zeek/iot-23-logs/"

# Iterate over each CTU* folder
for folder in "$source_folder"/*; do
    if [ -d "$folder" ]; then
        # Get the folder name
        folder_name=$(basename "$folder")
        
        # Find the pcap file in the folder (including subfolders)
        pcap_file=$(find "$folder" -type f -name "*.pcap" -print -quit)
        
        # Check if a pcap file exists
        if [ -n "$pcap_file" ]; then
            echo "Processing $pcap_file..."
            
            # Run Zeek on the pcap file and save logs in the corresponding folder
            zeek -C -r "$pcap_file" /usr/local/zeek/share/zeek/site/local.zeek Log::default_logdir="$destination_folder/$folder_name/"
            
            echo "Logs saved in $destination_folder/$folder_name/"
        else
            echo "No pcap file found in $folder"
        fi
    fi
done
