import pandas as pd
from sklearn.preprocessing import LabelEncoder
import time
import pandas as pd

# Define the CSV file path
input_csv_path = "/root/bbdd/logs-zeek/cic-iot-2023-logs/labeled-csv_all.csv"
output_csv_path = '/root/bbdd/logs-zeek/cic-iot-2023-encoded-common-12gb.csv'


# Columns to drop
columns_to_drop = ['tunnel_parents', 'ts', 'uid', 'id.orig_h', 'id.resp_h', 'id.orig_p', 'id.resp_p', 'startTime']

# Clean the DataFrame
def clean_dataframe(df):
    # Replace commas in 'service' column
    df['service'] = df['service'].str.replace(',', '-')

    # List of numeric and string columns
    cols_num = ['duration', 'orig_bytes', 'resp_bytes', 'missed_bytes',
                'orig_pkts', 'orig_ip_bytes', 'resp_pkts', 'resp_ip_bytes',
                'orig_bytes_mean', 'resp_bytes_mean', 'orig_bytes_std',
                'resp_bytes_std', 'orig_bytes_mean_nocero', 'resp_bytes_mean_nocero',
                'orig_bytes_std_nocero', 'resp_bytes_std_nocero', 'orig_bytes_min',
                'resp_bytes_min', 'orig_bytes_max', 'resp_bytes_max',
                'orig_pkts_nocero', 'resp_pkts_nocero', 'orig_pkts_cero',
                'resp_pkts_cero', 'time_mean', 'time_std', 'time_min', 'time_max',
                'orig_time_mean', 'orig_time_std', 'orig_time_min', 'orig_time_max',
                'resp_time_mean', 'resp_time_std', 'resp_time_min', 'resp_time_max']

    cols_str = ['proto', 'service']
    cols_dash = ['conn_state', 'local_orig', 'local_resp', 'history']

    # Clean numeric columns
    for col in cols_num:
        df[col] = df[col].fillna('0').replace(['-', '', '[]', '<NA>'], '0').astype('float64')

    # Clean string columns
    for col in cols_str:
        df[col] = df[col].fillna('unknown').replace(['-', '', '[]', '<NA>'], 'unknown').astype('object')

    for col in cols_dash:
        df[col] = df[col].fillna('-').replace(['', '[]', '<NA>'], '-').astype('object')

    # Replace label values
    df['label'] = df['label'].str.replace('Mirai', 'DoS').str.replace('Recon', 'Scan').str.replace('Scanning', 'Scan') \
        .str.replace('DDoS', 'DoS').str.replace('DictionaryBruteForce', 'BruteForce')

    return df

# Normalize local values
def normalize_local(value):
    if value in [True, 'True', 'T']:
        return 'True'
    elif value in [False, 'False', 'F']:
        return 'False'
    else:
        return value

# Load values for encoding
history_values = []
with open('history_values.txt', 'r') as f:
    for line in f:
        history_values.append(line.strip())

service_values = ['unknown', 'dns', 'http', 'ssl', 'ntp', 'gssapi-smb', 'dhcp', 'krb_tcp', 'xmpp', 'ldap_udp', 'geneve', 'radius', 'ssh', 'syslog', 'vxlan', 'mqtt', 'ayiya', 'ssl-quic', 'quic-ssl', 'ssl-http', 'irc']

conn_state_values = ["S0", "S1", "SF", "REJ", "S2", "S3", "RSTO", "RSTR", 
                     "RSTOS0", "RSTRH", "SH", "SHR", "OTH", "-"]

local_values = ["True", "False"]
proto_values = ["tcp", "udp", "icmp", "unknown"]

# Fit encoders for known unique value columns
le_history = LabelEncoder()
le_history.fit(history_values)

le_service = LabelEncoder()
le_service.fit(service_values)

le_conn_state = LabelEncoder()
le_conn_state.fit(conn_state_values)

le_local_resp = LabelEncoder()
le_local_resp.fit(local_values)

le_local_orig = LabelEncoder()
le_local_orig.fit(local_values)

le_proto = LabelEncoder()
le_proto.fit(proto_values)

# Create a dictionary of encoders
encoders = {
    'conn_state': le_conn_state,
    'local_resp': le_local_resp,
    'local_orig': le_local_orig,
    'proto': le_proto,
    'service': le_service,
    'history': le_history
}

columns_to_encode = ['proto', 'service', 'history', 'conn_state', 'local_orig', 'local_resp']

# Encode columns
def encode_columns(df, columns_to_encode, encoders):
    for col in columns_to_encode:
        le = encoders[col]
        df[col] = le.transform(df[col])
    return df

# Read and process the CSV file in chunks
chunk_size = 50000
chunks = pd.read_csv(input_csv_path, dtype=str, chunksize=chunk_size)

# Write header to the output CSV
first_chunk = next(chunks)
first_chunk = first_chunk.drop(columns_to_drop, axis=1)
first_chunk = clean_dataframe(first_chunk)
first_chunk['local_orig'] = first_chunk['local_orig'].apply(normalize_local)
first_chunk['local_resp'] = first_chunk['local_resp'].apply(normalize_local)
first_chunk = encode_columns(first_chunk, columns_to_encode, encoders)
first_chunk.to_csv(output_csv_path, mode='w', index=False, header=True)

# Function to sample 30% of the DoS labeled rows
def sample_majority_class(df, label_col, majority_class, frac, random_state=None):
    majority_df = df[df[label_col] == majority_class]
    minority_df = df[df[label_col] != majority_class]
    
    sampled_majority_df = majority_df.sample(frac=frac, random_state=random_state)
    
    return pd.concat([sampled_majority_df, minority_df], ignore_index=True)

# Process and append remaining chunks
for chunk in chunks:
    chunk = chunk.drop(columns_to_drop, axis=1)
    chunk = clean_dataframe(chunk)
    chunk['local_orig'] = chunk['local_orig'].apply(normalize_local)
    chunk['local_resp'] = chunk['local_resp'].apply(normalize_local)
    chunk = encode_columns(chunk, columns_to_encode, encoders)
    sampled_chunk = sample_majority_class(chunk, label_col='label', majority_class='DoS', frac=0.3, random_state=42)
    sampled_chunk.to_csv(output_csv_path, mode='a', index=False, header=False)
    print(f"Chunk appended.")

print(f'DataFrame saved to {output_csv_path}')