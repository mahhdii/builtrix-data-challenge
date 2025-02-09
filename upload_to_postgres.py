import os
import pg8000.native  # Using pg8000 for PostgreSQL
import csv

# Database credentials
DB_NAME = "builtrix_db"
DB_USER = "mahhdii"
DB_PASSWORD = "builtrix"
DB_HOST = "localhost"
DB_PORT = 5432

# Define paths
CLEANED_DIR = "data/cleaned"



def check_csv_headers(file_path):
    with open(file_path, "r") as f:
        reader = csv.reader(f)
        headers = next(reader)  # Get the first row (header)
        print(f"Headers for {file_path}: {headers}")
    return headers



def copy_csv_to_postgres(table_name, file_path, column_mapping):
    conn = None  # Initialize conn to avoid UnboundLocalError
    try:
        print(f"Connecting to {DB_NAME} to insert into {table_name}...")


        headers = check_csv_headers(file_path)

        conn = pg8000.native.Connection(user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT,
                                        database=DB_NAME)


        with open(file_path, "r") as f:
            next(f)  # Skip header row
            data = f.read()


        sql_command = f"COPY {table_name} ({', '.join(column_mapping)}) FROM STDIN WITH CSV HEADER"
        conn.run(sql_command, data)

        print(f"{table_name} loaded successfully!")

    except Exception as e:
        print(f"Error loading {table_name}: {e}")

    finally:
        if conn:
            conn.close()
            print("🔌 Connection closed.")



copy_csv_to_postgres(
    "power_table",
    os.path.join(CLEANED_DIR, "HVAC_Power_kW.csv"),
    ["timestamp", "HVAC_Power_kW", "time_diff"]
)


copy_csv_to_postgres(
    "cfp_table",
    os.path.join(CLEANED_DIR, "processed_cfp_data_2022.csv"),
    [
        "timestamp", "renewable_biomass", "renewable_hydro", "renewable_solar", "renewable_wind", "renewable_geothermal",
        "renewable_otherrenewable", "renewable", "nonrenewable_coal", "nonrenewable_gas", "nonrenewable_nuclear",
        "nonrenewable_oil", "nonrenewable", "hydropumpedstorage", "unknown", "region_id", "country_id", "month",
        "day", "time_diff"
    ]
)


temperature_files = [f for f in os.listdir(CLEANED_DIR) if "AHU" in f]
for temp_file in temperature_files:
    copy_csv_to_postgres(
        "temperature_table",
        os.path.join(CLEANED_DIR, temp_file),
        [
            "timestamp", "time_diff", "AHU_01_FreshAir_Temp_C", "AHU_01_SupplyAir_Temp_C", "AHU_01_Exhaust_Temp_C",
            "AHU_02_FreshAir_Temp_C", "AHU_02_SupplyAir_Temp_C", "AHU_02_Exhaust_Temp_C", "AHU_N01_FreshAir_Temp_C",
            "AHU_N01_SupplyAir_Temp_C", "AHU_N02_FreshAir_Temp_C", "AHU_N02_SupplyAir_Temp_C",
            "AHU_N03_FreshAir_Temp_C", "AHU_N03_SupplyAir_Temp_C"
        ]
    )

print("All data loaded successfully into PostgreSQL!")