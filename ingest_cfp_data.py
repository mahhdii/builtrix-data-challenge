import os
import requests
import pandas as pd

# Define directories
RAW_CFP_DIR = "data/raw/cfp_data_2022"
PROCESSED_DIR = "data/processed"
PROCESSED_CFP_FILE = os.path.join(PROCESSED_DIR, "processed_cfp_data_2022.csv")


os.makedirs(RAW_CFP_DIR, exist_ok=True)
os.makedirs(PROCESSED_DIR, exist_ok=True)

cfp_dataframes = []

print("Starting download of carbon footprint data for 2022...")

# Loop through each month and day
for month in range(1, 13):  # Months 01 to 12
    for day in range(1, 32):  # Days 01 to 31
        month_str = str(month).zfill(2)
        day_str = str(day).zfill(2)


        cfp_url = f"https://buitrix-challenge-01.s3.amazonaws.com/cfp-data/month={month}/day={day}/entsoe.csv"
        local_file_path = os.path.join(RAW_CFP_DIR, f"entsoe_2022_{month_str}_{day_str}.csv")

        try:
            response = requests.get(cfp_url)
            if response.status_code == 200:
                with open(local_file_path, 'wb') as f:
                    f.write(response.content)
                print(f"Downloaded: {cfp_url}")

                # Load data and add month/day columns
                df = pd.read_csv(local_file_path)
                df["month"] = month
                df["day"] = day
                cfp_dataframes.append(df)
            else:
                print(f"No data found for {cfp_url}, skipping...")
        except Exception as e:
            print(f"Error downloading {cfp_url}: {e}")


if cfp_dataframes:
    cfp_df = pd.concat(cfp_dataframes, ignore_index=True)
    cfp_df.to_csv(PROCESSED_CFP_FILE, index=False)
    print(f"Processed carbon footprint data saved to {PROCESSED_CFP_FILE}")
else:
    print("No valid carbon footprint data found for 2022.")
