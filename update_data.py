import pandas as pd
import os

# --- Configuration ---
csv_file_path = "daily_upload/daily_stock_data.csv"
parquet_file_path = "master_data.parquet"

# --- Script Logic ---
print("Starting daily data update...")

if not os.path.exists(csv_file_path):
    print(f"Daily CSV not found at {csv_file_path}. No update to perform. Exiting.")
    exit()

df_new = pd.read_csv(csv_file_path)
print(f"Successfully loaded {len(df_new)} new rows from CSV.")

if os.path.exists(parquet_file_path):
    df_master = pd.read_parquet(parquet_file_path)
    print(f"Loaded {len(df_master)} existing rows from Parquet file.")
else:
    print("Master Parquet file not found. A new one will be created.")
    df_master = pd.DataFrame()

df_combined = pd.concat([df_master, df_new], ignore_index=True)
df_combined.drop_duplicates(subset=['SYMBOL', 'DATE'], keep='last', inplace=True)
print(f"After removing duplicates, final dataset has {len(df_combined)} rows.")

df_combined.to_parquet(parquet_file_path, index=False)
print(f"Successfully updated and saved the master Parquet file.")

# Clean up the daily csv after processing
os.remove(csv_file_path)
print("Removed daily CSV file.")
