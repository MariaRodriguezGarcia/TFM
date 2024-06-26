import os
import pandas as pd

def concatenate_csv_files(main_directory, save_directory, chunk_size=50000):
    """
    Concatenate all CSV files in subdirectories of the main directory into a single CSV file.

    Parameters:
    main_directory (str): Path to the main directory containing subdirectories with CSV files.
    save_directory (str): Path to the directory where the concatenated CSV file will be saved.
    chunk_size (int): Number of rows per chunk to read from each CSV file.
    """
    
    # Get the main directory name for the output file
    main_directory_name = os.path.basename(os.path.normpath(main_directory))
    output_file = os.path.join(save_directory, f'{main_directory_name}_all.csv')
    
    # Initialize a flag to indicate whether to write header
    header_written = False
    column_order = []
    
    total_length = 0  # Initialize total length counter
    
    # Walk through each subfolder in the main directory
    for subdir, _, files in os.walk(main_directory):
        for file in files:
            # Check if the file is a CSV file
            if file.endswith('.csv'):
                file_path = os.path.join(subdir, file)
                
                # Process the CSV file in chunks
                for chunk in pd.read_csv(file_path, chunksize=chunk_size):
                    if not header_written:
                        # Write the first chunk with headers and save column order
                        chunk.to_csv(output_file, mode='w', header=True, index=False)
                        header_written = True
                        column_order = chunk.columns.tolist()
                    else:
                        # Ensure the chunk has the same column order and write without headers
                        chunk = chunk.reindex(columns=column_order)
                        chunk.to_csv(output_file, mode='a', header=False, index=False)
                    
                    total_length += len(chunk)  # Add length of current chunk to total length

    print(f'Total length of concatenated CSV: {total_length}')
    print(f'All CSV files have been concatenated and saved to {output_file}')


main_directory = "/root/bbdd/logs-zeek/cic-iot-2023-logs/labeled-csv/"
save_directory = "/root/bbdd/logs-zeek/cic-iot-2023-logs/"
concatenate_csv_files(main_directory,save_directory)
