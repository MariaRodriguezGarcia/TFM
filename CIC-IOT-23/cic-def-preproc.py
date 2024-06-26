import dask.dataframe as dd
import dask
import time
import pandas as pd
from dask.distributed import Client

def main(): 


    from dask_ml.model_selection import train_test_split
    from dask_ml.preprocessing import StandardScaler
    from dask_ml.impute import SimpleImputer
    from sklearn.ensemble import RandomForestClassifier
    from sklearn.preprocessing import LabelEncoder
    with Client(n_workers=6, scheduler_port=8786, dashboard_address=':8787') as client:
        dtypes = {
            'duration': 'float64', 
            'orig_bytes': 'float64', 
            'resp_bytes': 'float64', 
            'missed_bytes': 'float64', 
            'orig_pkts': 'float64', 
            'orig_ip_bytes': 'float64', 
            'resp_pkts': 'float64', 
            'resp_ip_bytes': 'float64', 
            'orig_bytes_mean': 'float64', 
            'resp_bytes_mean': 'float64', 
            'orig_bytes_std': 'float64', 
            'resp_bytes_std': 'float64', 
            'orig_bytes_mean_nocero': 'float64', 
            'resp_bytes_mean_nocero': 'float64', 
            'orig_bytes_std_nocero': 'float64', 
            'resp_bytes_std_nocero': 'float64', 
            'orig_bytes_min': 'float64', 
            'resp_bytes_min': 'float64', 
            'orig_bytes_max': 'float64', 
            'resp_bytes_max': 'float64', 
            'orig_pkts_nocero': 'float64', 
            'resp_pkts_nocero': 'float64', 
            'orig_pkts_cero': 'float64', 
            'resp_pkts_cero': 'float64', 
            'time_mean': 'float64', 
            'time_std': 'float64', 
            'time_min': 'float64', 
            'time_max': 'float64', 
            'orig_time_mean': 'float64', 
            'orig_time_std': 'float64', 
            'orig_time_min': 'float64', 
            'orig_time_max': 'float64', 
            'resp_time_mean': 'float64', 
            'resp_time_std': 'float64', 
            'resp_time_min': 'float64', 
            'resp_time_max': 'float64',
            'proto': 'object', 
            'service': 'object',
            'conn_state': 'object', 
            'local_orig': 'object', 
            'local_resp': 'object',
            'history': 'object'
        }
        # Define the CSV file path
        cic_path = "/root/bbdd/logs-zeek/cic-iot-2023-logs/labeled-csv_all.csv"
        import dask.dataframe as dd

        ddf = dd.read_csv(cic_path, dtype=str)#, dtype=dtypes)
        print("CSV file read into Dask DataFrame.")
        columns_to_drop = ['tunnel_parents', 'ts', 'uid', 'id.orig_h', 'id.resp_h', 'id.orig_p', 'id.resp_p', 'startTime']
        ddf = ddf.drop(columns_to_drop, axis=1)
        print("Unnecessary columns dropped.")


        def clean_dataframe(ddf):
            # Replace commas in 'service' column
            ddf['service'] = ddf['service'].str.replace(',', '-')

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
                ddf[col] = ddf[col].fillna('0').replace(['-', '', '[]', '<NA>'], '0').astype('float64')

            # Clean string columns
            for col in cols_str:
                ddf[col] = ddf[col].fillna('unknown').replace(['-', '', '[]', '<NA>'], 'unknown').astype('object')
            
            for col in cols_dash:
                ddf[col] = ddf[col].fillna('-').replace(['', '[]', '<NA>'], '-').astype('object')
            
            # Replace label values
            ddf['label'] = ddf['label'].str.replace('Mirai', 'DoS').str.replace('Recon', 'Scan').str.replace('Scanning', 'Scan') \
                .str.replace('DDoS', 'DoS').str.replace('DictionaryBruteForce', 'BruteForce')

            return ddf

        output_csv_path = '/root/bbdd/logs-zeek/cic-iot-2023-processed-12gb.csv'
        ddf.columns = ddf.columns.map(str)
        ddf = clean_dataframe(ddf)
        columns_to_encode = ['proto', 'service', 'history', 'conn_state', 'local_orig', 'local_resp']
        def encode_columns(dask_df, columns_to_encode):
            le = LabelEncoder()
            for col in columns_to_encode:
                dask_df[col] = dask_df[col].map_partitions(lambda part: pd.Series(le.fit_transform(part), index=part.index), meta=(col, 'int64'))
            return dask_df
        ddf = encode_columns(ddf, columns_to_encode)
        print("Label encoding applied in parallel.")

        sample_frac = 0.3
        ddf = ddf.sample(frac=sample_frac, random_state=42)


        #LECTURA BUENA
        header = ddf.columns.tolist()
        with open(output_csv_path, 'w') as f:
            f.write(','.join(header) + '\n')
        import pandas as pd
            # Step 2: Iterate over each partition and append to the CSV file
        print(ddf.npartitions)
        """ddf.to_csv(output_csv_path, index=False, single_file=True)"""

        for i in range(ddf.npartitions):
            # Compute the partition to get a pandas DataFrame
            chunk = ddf.get_partition(i).compute()
            # Append to the CSV file without writing the header again
            chunk.to_csv(output_csv_path, mode='a', index=False, header=False)
            print(f"{i} Chunk written.")

        print(f'DataFrame saved to {output_csv_path}')
if __name__ == "__main__":
    main()