import os
import pandas as pd

# Define the path to the folder containing the Parquet files
parquet_folder = '/Users/sarah/Desktop/ccnews-dataset/'

# Loop through each file in the folder
for file_name in os.listdir(parquet_folder):
    if file_name.endswith('.parquet'):
        # Define full file paths
        parquet_file = os.path.join(parquet_folder, file_name)
        csv_file = os.path.join(parquet_folder, file_name.replace('.parquet', '.csv'))

        # Read the Parquet file
        df = pd.read_parquet(parquet_file)

        # Write the DataFrame to a CSV file
        df.to_csv(csv_file, index=False)

        print(f"Parquet file {file_name} converted to CSV and saved as {csv_file}")
