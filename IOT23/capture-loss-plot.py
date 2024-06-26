import os
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from bokeh.plotting import figure, show, output_file
from bokeh.models import HoverTool
from bokeh.io import export_png

main_directory = "/root/bbdd/logs-zeek/iot-23-logs/labeled-csv/"

def process_conn_log(folder_name, thresholds=[(0, 0.05), (0.05, 0.1), (0.1, 0.2), (0.2, 1)], columns=["uid", "missed_bytes", "orig_bytes","resp_bytes", "id.orig_h", "id.orig_p", "id.resp_h", "id.resp_p", "proto", 'binary-label']):
    conn_log_path = os.path.join(main_directory, folder_name)
    
    # Check if conn.log file exists
    if os.path.exists(conn_log_path):
        
        # Create Bokeh plot
        p = figure(title=f"Evolution of Missed Bytes Ratio - {folder_name}", x_axis_label="Rank Position (sorted by missed ratio)", y_axis_label="Missed Bytes Ratio")

        # Add hover tool
        hover = HoverTool()
        hover.tooltips = [("Index", "$index"), ("Missed Bytes Ratio", "@missed_ratio")]
        p.add_tools(hover)


        for chunk in pd.read_csv(conn_log_path, usecols=columns, chunksize=50000):
            # Calculate missed_bytes ratio
            chunk['missed_ratio'] = np.where((chunk['missed_bytes'].isna()) | (chunk['missed_bytes'] == 0), 0, pd.to_numeric(chunk['missed_bytes']) / (pd.to_numeric(chunk['orig_bytes']) + pd.to_numeric(chunk['resp_bytes'])))
            if chunk['missed_ratio'].isnull().values.any():
                print("Warning: NaN value detected in missed_ratio column!")
            # Define color based on binary-label
            colors = ['green' if label == 0 else 'red' for label in chunk['binary-label']]
            # Add scatter plot for the chunk
            p.scatter(list(range(1, len(chunk) + 1)), chunk["missed_ratio"], size=10, color=colors, alpha=0.5)

        # Show plot for the chunk
        export_png(p,filename=f"{folder_name}.png")
        show(p)
        print(f"Saved in: {folder_name}")

    else:
        print(f"conn.log not found in {folder_path}")

json_files = [f for f in os.listdir(main_directory) if f.startswith("json")]
for json_file in json_files:
        process_conn_log(json_file)
