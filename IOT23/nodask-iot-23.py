import dask.dataframe as dd
import dask
import time
import pandas as pd
def find_columns_with_missing_values_partition(partition):
    """
    Find columns in the partition that have missing values, such as NaNs, empty strings, or "-".

    Parameters:
    partition (pd.DataFrame): The partition to check.

    Returns:
    list: A list of column names that contain missing values.
    """
    missing_value_columns = set()

    # Check for NaNs
    nan_columns = partition.isna().any()
    missing_value_columns.update(nan_columns[nan_columns].index.tolist())

    # Check for empty strings, "-", or "[] or <NA>"
    for value in ["", "-", "[]",'<NA>']:
        value_columns = (partition == value).any()
        missing_value_columns.update(value_columns[value_columns].index.tolist())

    return list(missing_value_columns)

####################

def find_columns_with_missing_values(df):
    """
    Find columns in the DataFrame that have missing values, such as NaNs, empty strings, or "-".

    Parameters:
    df (dd.DataFrame): The Dask DataFrame to check.

    Returns:
    list: A list of column names that contain missing values.
    """
    partitions = df.to_delayed()
    results = [dask.delayed(find_columns_with_missing_values_partition)(part) for part in partitions]
    combined_results = dask.compute(*results)
    
    # Combine results from all partitions
    missing_value_columns = set()
    for result in combined_results:
        missing_value_columns.update(result)
    
    return list(missing_value_columns)

def check_missing_value_types_partition(partition, columns):
    """
    Check for the types of missing values in the specified columns of the partition.

    Parameters:
    partition (pd.DataFrame): The partition to check.
    columns (list): The list of column names to check for missing values.

    Returns:
    dict: A dictionary where keys are column names and values are sets of missing value types.
    """
    missing_value_types = {}

    for column in columns:
        types = set()

        if partition[column].isna().any():
            types.add("NaN")

        if (partition[column] == "").any():
            types.add('""')

        if (partition[column] == "-").any():
            types.add('"-"')

        if (partition[column] == "[]").any():
            types.add('"[]"')

        if (partition[column] == '<NA>').any():
            types.add('"<NA>"')

        missing_value_types[column] = types

    return missing_value_types

#######################

def check_missing_value_types(df, columns):
    """
    Check for the types of missing values in the specified columns.

    Parameters:
    df (dd.DataFrame): The Dask DataFrame to check.
    columns (list): The list of column names to check for missing values.

    Returns:
    dict: A dictionary where keys are column names and values are sets of missing value types.
    """
    partitions = df.to_delayed()
    results = [dask.delayed(check_missing_value_types_partition)(part, columns) for part in partitions]
    combined_results = dask.compute(*results)

    # Combine results from all partitions
    missing_value_types = {}
    for result in combined_results:
        for column, types in result.items():
            if column not in missing_value_types:
                missing_value_types[column] = types
            else:
                missing_value_types[column].update(types)

    return missing_value_types
def replace_missing_values_partition(partition, column_names, nan_value=None, empty_value=None, placeholder_value=None):
    """
    Replace missing values in specified columns of the partition with different values for NaNs, empty strings, and placeholders.

    Parameters:
    partition (pd.DataFrame): The partition containing the columns.
    column_names (str or list): The name(s) of the columns to replace values in.
    nan_value: The value to replace NaNs with.
    empty_value: The value to replace empty strings with.
    placeholder_value: The value to replace specific placeholder values with.

    Returns:
    pd.DataFrame: The partition with the replaced values.
    """
    if isinstance(column_names, str):
        column_names = [column_names]

    for column_name in column_names:
        if column_name in partition.columns:
            if nan_value is not None:
                partition[column_name] = partition[column_name].fillna(nan_value)
            if empty_value is not None:
                partition[column_name] = partition[column_name].replace("", empty_value)
            if placeholder_value is not None:
                partition[column_name] = partition[column_name].replace("-", placeholder_value)
                partition[column_name] = partition[column_name].replace("[]", placeholder_value)
                partition[column_name] = partition[column_name].replace('<NA>', placeholder_value)
        else:
            print(f"Column '{column_name}' does not exist in the partition.")

    return partition

#########################

def replace_missing_values(df, column_names, nan_value=None, empty_value=None, placeholder_value=None):
    """
    Replace missing values in specified columns of the DataFrame with different values for NaNs, empty strings, and placeholders.

    Parameters:
    df (dd.DataFrame): The Dask DataFrame containing the columns.
    column_names (str or list): The name(s) of the columns to replace values in.
    nan_value: The value to replace NaNs with.
    empty_value: The value to replace empty strings with.
    placeholder_value: The value to replace specific placeholder values with.

    Returns:
    dd.DataFrame: The DataFrame with the replaced values.
    """
    partitions = df.to_delayed()
    results = [dask.delayed(replace_missing_values_partition)(part, column_names, nan_value, empty_value, placeholder_value) for part in partitions]
    return dd.from_delayed(results)

    
    """
    Replace missing values in specified columns of the DataFrame with different values for NaNs, empty strings, and placeholders.

    Parameters:
    df (pd.DataFrame): The DataFrame containing the columns.
    column_names (str or list): The name(s) of the columns to replace values in.
    nan_value: The value to replace NaNs with.
    empty_value: The value to replace empty strings with.
    placeholder_value: The value to replace specific placeholder values with.

    Returns:
    pd.DataFrame: The DataFrame with the replaced values.
    """
    if isinstance(column_names, str):
        column_names = [column_names]

    for column_name in column_names:
        if column_name in df.columns:
            if nan_value is not None:
                df[column_name] = df[column_name].fillna(nan_value)
            if empty_value is not None:
                df[column_name] = df[column_name].replace("", empty_value)
            if placeholder_value is not None:
                df[column_name] = df[column_name].replace("-", placeholder_value)
        else:
            print(f"Column '{column_name}' does not exist in the DataFrame.")

    return df


import dask.dataframe as dd
from dask_ml.model_selection import train_test_split
from dask_ml.preprocessing import StandardScaler
from dask_ml.impute import SimpleImputer
from sklearn.ensemble import RandomForestClassifier
from sklearn.preprocessing import LabelEncoder

# Define the CSV file path
iot23_path = "/root/bbdd/logs-zeek/iot-23-logs/labeled-csv_all.csv"
dtypes = {'service': 'str'}

from dask.distributed import Client

def main():
    import dask.dataframe as dd
    # Create a Dask client with multiple workers
    with Client(n_workers=6, scheduler_port=8786, dashboard_address=':8787') as client:
        print("Dask Client created.")

        ddf = dd.read_csv(iot23_path, dtype=dtypes)
        print("CSV file read into Dask DataFrame.")

        columns_to_drop = ['tunnel_parents', 'ts', 'uid', 'id.orig_h', 'id.resp_h', 'id.orig_p', 'id.resp_p', 'startTime']
        ddf = ddf.drop(columns_to_drop, axis=1)
        print("Unnecessary columns dropped.")

        target_column = 'label'  # Replace 'target_column' with your actual target column name

        # Filter rows where the label contains 'DDoS', 'PartOfAHorizontalPortscan', or 'benign'
        ddf = ddf[ddf[target_column].str.contains('DDoS|PartOfAHorizontalPortscan|benign')]
        print("Rows filtered based on label column.")

        # Change labels 'DDoS' to 'DoS' and 'PartOfAHorizontalPortscan' to 'Scan'
        def map_label(label):
            if 'DDoS' in label:
                return 'DoS'
            elif 'PartOfAHorizontalPortscan' in label:
                return 'Scan'
            else:
                return label

        ddf[target_column] = ddf[target_column].apply(map_label, meta=(target_column, 'str'))
        print("Labels mapped to simplified categories.")


        # Impute missing values with 0 for numerical columns and '-' for categorical columns
        columns_to_fill_zero = ['orig_time_std', 'resp_bytes_std', 'duration', 'orig_bytes', 'resp_time_std', 'resp_bytes', 'orig_bytes_std']
        columns_to_fill_dash = ['history']
        columns_to_fill_unkwn = ['service']

        # Fill NaN values in columns_to_fill_zero with 0
        ddf = ddf.fillna({col: 0 for col in columns_to_fill_zero})

        # Fill NaN values in columns_to_fill_dash with '-'
        ddf = ddf.fillna({col: "-" for col in columns_to_fill_dash})

        # Fill NaN values in columns_to_fill_unkwn with 'unknown'
        ddf = ddf.fillna({col: "unknown" for col in columns_to_fill_unkwn})
        ddf['service'] = ddf['service'].replace('<NA>', 'unknown')
        ddf['history'] = ddf['history'].replace('<NA>', '-')
        print("Missing values imputed.")

        def normalize_local(value):
            if value in [True, 'True', 'T']:
                return 'True'
            elif value in [False, 'False', 'F']:
                return 'False'
            else:
                return value

        ddf['local_orig'] = ddf['local_orig'].apply(normalize_local, meta=('local_orig', 'object'))
        ddf['local_resp'] = ddf['local_resp'].apply(normalize_local,  meta=('local_resp', 'object'))

        """
        start_time = time.time()
        missing_value_columns = find_columns_with_missing_values(ddf)
        print("Columns with missing values:", missing_value_columns)

        # Checking types of missing values
        missing_value_types = check_missing_value_types(ddf, missing_value_columns)
        print("Missing value types:", missing_value_types)

        for column, types in missing_value_types.items():
            print(f"Column '{column}' has the following types of missing values: {types}")

        print("Time taken for missing values:", time.time() - start_time, "seconds")
        """
        """

    
        Columns with missing values: ['history']
        Missing value types: {'history': {'"-"'}}
        Column 'history' has the following types of missing values: {'"-"'}
        Time taken for missing values: 350.23516821861267 seconds

        
        
        
        # Assuming df is your DataFrame
        columns_to_check = ['proto', 'service', 'history', 'conn_state', 'local_orig', 'local_resp']

        # Check for NaN values in the specified columns
        nan_check = ddf[columns_to_check].isna().any().compute()

        # Iterate over nan_check to print the result
        for column, has_nan in nan_check.items():
            print(f"Column '{column}' has NaN values: {has_nan}")

        
        Column 'proto' has NaN values: False
        Column 'service' has NaN values: False
        Column 'history' has NaN values: False
        Column 'conn_state' has NaN values: False
        Column 'local_orig' has NaN values: False
        Column 'local_resp' has NaN values: False
        
        print(ddf['service'].unique().compute())
        print(ddf['proto'].unique().compute())
        """        
        start_time = time.time()
        # Apply label encoding with different encoders for different columns in parallel


        def encode_columns(dask_df, columns_to_encode):
            for col in columns_to_encode:
                le = LabelEncoder()
                dask_df[col] = dask_df[col].map_partitions(le.fit_transform, meta=(col, 'int'))
            return dask_df

        
        columns_to_encode = ['proto', 'service', 'history', 'conn_state', 'local_orig', 'local_resp']
        ddf = encode_columns(ddf, columns_to_encode)
        print(type(ddf))  # Check the type of ddf
        """  
        def apply_label_encoding(df, column_name):
            label_encoder = LabelEncoder()
            encoded_values = label_encoder.fit_transform(df[column_name])
            encoded_df = pd.DataFrame({column_name: encoded_values.compute()}, index=df.index)
            return dd.from_pandas(encoded_df, npartitions=df.npartitions)
        
        # Apply label encoding to each specified column
        ddf['proto'] = apply_label_encoding(ddf, 'proto')
        print("Label encoding applied to proto.")

        ddf['service'] = apply_label_encoding(ddf, 'service')
        print("Label encoding applied to service.")

        ddf['history'] = apply_label_encoding(ddf, 'history')
        print("Label encoding applied to history.")

        ddf['conn_state'] = apply_label_encoding(ddf, 'conn_state')
        print("Label encoding applied to conn_state.")

        ddf['local_orig'] = apply_label_encoding(ddf, 'local_orig')
        print("Label encoding applied to local_orig.")

        ddf['local_resp'] = apply_label_encoding(ddf, 'local_resp')
        print("Label encoding applied to local_resp.")
        """  
        print("Label encoding applied in parallel.")
        print("Time taken for label encoding:", time.time() - start_time, "seconds")
        
        # Split data into features and target
        X = ddf.drop(target_column, axis=1)
        y = ddf[target_column]
        """
        # Print column names and types
        print("Column names and types:")
        # Print column names and their types
        for column_name in ddf.columns:
            print("Column:", column_name, ", Type:", type(column_name))

        
        start_time = time.time()
        # Print unique values of label, service, and history columns
        print("Unique values of 'label' column:")
        print(y.unique().compute())

        print("Time taken for unique value printing:", time.time() - start_time, "seconds")
        

        # Split the data into train and test sets with shuffle=True
        x_train, x_test, y_train, y_test = train_test_split(X, y, test_size=0.6, random_state=42, shuffle=True)
        print("Data split into train and test sets.")
        """
        output_csv_path = '/root/bbdd/logs-zeek/iot23-processed.csv'
        ddf.columns = ddf.columns.map(str)



         #LECTURA BUENA
        header = ddf.columns.tolist()
        with open(output_csv_path, 'w') as f:
            f.write(','.join(header) + '\n')

        # Step 2: Iterate over each partition and append to the CSV file
        for i in range(ddf.npartitions):
            # Compute the partition to get a pandas DataFrame
            chunk = ddf.get_partition(i).compute()
            # Append to the CSV file without writing the header again
            chunk.to_csv(output_csv_path, mode='a', index=False, header=False)
            print("Chunk written.")

        print(f'DataFrame saved to {output_csv_path}')
        

if __name__ == "__main__":
    main()