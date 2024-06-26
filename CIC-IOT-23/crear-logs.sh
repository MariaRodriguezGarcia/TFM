
#!/bin/bash

# Path to the source folder containing pcap files
source_folder="/root/bbdd/cic-iot-2023/Mirai-greeth_flood/"

# Destination folder where logs will be saved
destination_folder="/root/bbdd/logs-zeek/cic-iot-2023-logs/mirai-greeth/"

# Iterate over each pcap file
for pcap_file in "$source_folder"/*.pcap; do
    if [ -f "$pcap_file" ]; then
        # Get the filename without extension
        folder_name=$(basename -s .pcap "$pcap_file")

        # Create a folder with the filename in the destination folder
        mkdir -p "$destination_folder/$folder_name"

        echo "Processing $pcap_file..."

        # Run Zeek on the pcap file and save logs in the corresponding folder
        zeek -C -r "$pcap_file" /usr/local/zeek/share/zeek/site/local.zeek Log::default_logdir="$destination_folder/$folder_name/"

        echo "Logs saved in $destination_folder/$folder_name/"
    fi
done
