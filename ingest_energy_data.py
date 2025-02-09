import os
import requests
import pandas as pd


DATA_DIR = "data/raw"
PROCESSED_DIR = "data/processed"
ENERGY_FILE_URL = "https://buitrix-challenge-01.s3.amazonaws.com/energy_data.xlsx"
ENERGY_FILE_PATH = os.path.join(DATA_DIR, "energy_data.xlsx")

# Ensure directories exist
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(PROCESSED_DIR, exist_ok=True)


print("Downloading energy data file...")
response = requests.get(ENERGY_FILE_URL)
if response.status_code == 200:
    with open(ENERGY_FILE_PATH, 'wb') as f:
        f.write(response.content)
    print(f"File downloaded and saved to {ENERGY_FILE_PATH}")
else:
    print("Failed to download energy data file.")
    exit(1)


print("Reading energy data...")
energy_data = pd.read_excel(ENERGY_FILE_PATH, sheet_name=None)

for sheet_name, df in energy_data.items():
    processed_file_path = os.path.join(PROCESSED_DIR, f"{sheet_name}.csv")
    df.to_csv(processed_file_path, index=False)
    print(f"Processed data saved to {processed_file_path}")

print("All sheets processed successfully.")
