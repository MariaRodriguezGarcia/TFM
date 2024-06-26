#!/bin/bash

# Directorio de origen
source_dir="/root/bbdd/iotd20/pcaps/mirai/split/"

# Directorio de destino
dest_dir="/root/bbdd/logs-zeek/iotd20-logs/"

# Obtener una lista de archivos en el directorio de origen
files=$(ls "$source_dir"*.pcap)

# Iterar sobre cada archivo
for file_with_extension in $files
do
    # Obtener el nombre del archivo sin la extensión
    filename=$(basename -- "$file_with_extension")
    filename_no_extension="${filename%.*}"

    # Crear el nombre de la carpeta de destino
    dest_folder="$dest_dir$filename_no_extension-logs"

    # Crear la carpeta de destino
    mkdir -p "$dest_folder"

    # Ejecutar Zeek en el archivo actual
    zeek -r "$file_with_extension" local Log::default_logdir="$dest_folder"
done
