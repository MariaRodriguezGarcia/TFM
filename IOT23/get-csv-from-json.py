import sys
import json
import pandas as pd
from datetime import datetime

# Rutas de los archivos de entrada y salida
zeek_log_path = r"/root/bbdd/iot-23/CTU-IoT-Malware-Capture-33-1/bro/conn-labeled.log"
csv_output_path = r"/root/bbdd/iot-23/CTU-IoT-Malware-Capture-33-1/bro/output.csv"

# Función para aplicar transformaciones a un chunk de datos
def apply_transformations(chunk):
    # Aplicar transformación a la columna 'ts'
    chunk['ts'] = chunk['ts'].apply(datetime.fromtimestamp)
    # Dividir la columna 'tunnel_parents label detailed-label'
    chunk[['tunnel_parents', 'label', 'detailed-label']] = chunk['tunnel_parents   label   detailed-label'].str.split('\s{3}', expand=True)
    # Eliminar la columna original
    chunk.drop(columns=['tunnel_parents   label   detailed-label'], inplace=True)
    return chunk

# Función para procesar un chunk de datos
def process_chunk(chunk):
    return apply_transformations(chunk)

# Tamaño del chunk
chunk_size = 50000

# Leer el archivo de registro de Zeek en chunks
with open(zeek_log_path, 'r') as file:
    header_line = file.readlines()[6].strip().split('\t')[1:]
chunks = pd.read_csv(zeek_log_path, sep='\t', skiprows=8, names=header_line, engine='python', chunksize=chunk_size)

# Aplicar transformaciones a cada chunk y concatenar los resultados
processed_chunks = [process_chunk(chunk) for chunk in chunks]
df = pd.concat(processed_chunks, ignore_index=True)

# Guardar el DataFrame resultante como archivo CSV
df.to_csv(csv_output_path, index=False)

# Imprimir mensaje de éxito
print("Archivo CSV guardado exitosamente.")