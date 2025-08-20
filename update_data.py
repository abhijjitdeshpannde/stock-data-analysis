import pandas as pd
import os
import json
from datetime import datetime

# --- Main Script Logic ---

def load_data_from_parquet(file_path="master_data.parquet"):
    """Loads the master stock data from a Parquet file."""
    if not os.path.exists(file_path):
        print(f"Error: The file '{file_path}' was not found.")
        print("Please ensure the master_data.parquet file is in the same directory as this script.")
        return None
    
    print(f"Reading data from '{file_path}'...")
    try:
        df = pd.read_parquet(file_path)
        print("Successfully loaded data.")
        return df
    except Exception as e:
        print(f"An error occurred while reading the Parquet file: {e}")
        return None

def create_api_files(master_df):
    """Creates individual JSON files for each stock to be used as an API."""
    if master_df is None or master_df.empty:
        print("Master DataFrame is empty. Cannot create API files.")
        return

    # Ensure the 'DATE' column is in the correct format
    if 'DATE' not in master_df.columns:
        print("Error: 'DATE' column not found in the Parquet file.")
        return
        
    master_df['DATE'] = pd.to_datetime(master_df['DATE'])

    output_dir = 'api'
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        print(f"Created directory: {output_dir}")

    # Get a list of all unique stock symbols
    symbols = master_df['SYMBOL'].unique()

    # --- Create a main index file ---
    # This file lists all available stock symbols, so the frontend knows what to fetch.
    index_data = {'symbols': sorted(list(symbols))}
    index_path = os.path.join(output_dir, 'index.json')
    with open(index_path, 'w') as f:
        json.dump(index_data, f, indent=4)
    print(f"Created index file at {index_path}")

    # --- Create a JSON file for each stock ---
    for symbol in symbols:
        stock_df = master_df[master_df['SYMBOL'] == symbol].copy()
        
        # Convert timestamp to ISO 8601 string format for JSON compatibility
        stock_df['DATE'] = stock_df['DATE'].dt.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        
        # Convert DataFrame to a JSON string in a records-orientated format
        # e.g., [{"col1": "val1", "col2": "val2"}, {"col1": "val3", "col2": "val4"}]
        json_data = stock_df.to_json(orient='records')
        
        # Save the JSON data to a file named after the symbol
        file_path = os.path.join(output_dir, f'{symbol}.json')
        with open(file_path, 'w') as f:
            f.write(json_data)
    
    print(f"Successfully created JSON API files for {len(symbols)} stocks in the '{output_dir}' directory.")


if __name__ == "__main__":
    # 1. Load the data from your local Parquet file
    master_dataframe = load_data_from_parquet()
    
    # 2. Create the JSON files for the "API" from the loaded data
    create_api_files(master_dataframe)

