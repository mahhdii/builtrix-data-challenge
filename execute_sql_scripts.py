import os
import pg8000.native  # Using pg8000 for PostgreSQL

# Database credentials
DB_NAME = "builtrix_db"
DB_USER = "mahhdii"
DB_PASSWORD = "builtrix"
DB_HOST = "localhost"
DB_PORT = 5432


SQL_DIR = "sql"

def execute_sql_file(file_path):
    conn = None
    try:
        print(f"Executing {file_path}...")


        conn = pg8000.native.Connection(user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT,
                                        database=DB_NAME)


        with open(file_path, "r") as f:
            sql_script = f.read()
            conn.run(sql_script)

        print(f"Successfully executed {file_path}")

    except Exception as e:
        print(f"Error executing {file_path}: {e}")

    finally:
        if conn:
            conn.close()
            print("Connection closed.")

sql_files = [ "02_data_aggregation.sql", "03_carbon_emission_view.sql"]
for sql_file in sql_files:
    execute_sql_file(os.path.join(SQL_DIR, sql_file))

print("All SQL scripts executed successfully!")
