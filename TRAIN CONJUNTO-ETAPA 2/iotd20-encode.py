import pandas as pd
from sklearn.preprocessing import LabelEncoder

def clean_iot2d0_dataframe(df):
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
    

    return df

df_iotd20 = pd.read_csv("/root/bbdd/logs-zeek/iotd20-logs/all-labeled-final_v2.csv", dtype=str)
y_iotd20_binary = df_iotd20['binary-label']
y_iotd20 = df_iotd20['label']
df_iotd20 = df_iotd20.drop(columns=['ts','startTime','uid','id.orig_h','id.orig_p','id.resp_h','id.resp_p','tunnel_parents'])
df_iotd20 = clean_iot2d0_dataframe(df_iotd20)

# Normalize 'local_orig' column values
def normalize_local(value):
    if value in [True, 'True', 'T']:
        return 'True'
    elif value in [False, 'False', 'F']:
        return 'False'
    else:
        return value

df_iotd20['local_orig'] = df_iotd20['local_orig'].apply(normalize_local)
df_iotd20['local_resp'] = df_iotd20['local_resp'].apply(normalize_local)

def update_labels(df):
    df.loc[(df['label'] == "Mirai") & (df['detailed-label'].str.contains('Telnet')), 'label'] = "BruteForce"
    df.loc[(df['label'] == "Mirai") & (~df['detailed-label'].str.contains('Telnet')), 'label'] = "DoS"
    return df

df_iotd20 = update_labels(df_iotd20)
df_iotd20 = df_iotd20.drop(columns=['detailed-label'])

# Load values for encoding
history_values = []
with open('/root/history_values.txt', 'r') as f:
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

df_iotd20 = encode_columns(df_iotd20, columns_to_encode, encoders)

# Save the sampled DataFrame to a CSV file
df_iotd20.to_csv('/root/bbdd/logs-zeek/encoded_iotd20_v2.csv', index=False)

