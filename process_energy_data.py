import os
import pandas as pd

# Define paths
PROCESSED_DIR = "data/processed"
OUTPUT_DIR = "data/cleaned"
REPORT_DIR = "data/reports"
os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(REPORT_DIR, exist_ok=True)

energy_files = [f for f in os.listdir(PROCESSED_DIR) if f.endswith(".csv")]

def calculate_quality(df):
    total = len(df)
    missing_count = df.isnull().sum()
    quality_score = ((total - missing_count) / total) * 100
    return quality_score


def process_energy_data(file_path):
    print(f"Processing {file_path}...")
    df = pd.read_csv(file_path)


    timestamp_col = None
    for col in ["timestamp", "Datetime", "datetime", "Timestamp"]:
        if col in df.columns:
            timestamp_col = col
            break

    if not timestamp_col:
        print(f"Skipping {file_path}: No timestamp column found.")
        return


    df.rename(columns={timestamp_col: "timestamp"}, inplace=True)


    df["timestamp"] = pd.to_datetime(df["timestamp"], dayfirst=True, errors="coerce")

    df.dropna(subset=["timestamp"], inplace=True)

    df = df.sort_values("timestamp")


    df["time_diff"] = df["timestamp"].diff().dt.total_seconds() / 60  # Convert to minutes


    large_gaps = df[df["time_diff"] > 120]
    if not large_gaps.empty:
        print(f"Warning: {len(large_gaps)} instances where data gaps exceed 2 hours in {file_path}")


    for col in df.columns:
        if col not in ["timestamp", "time_diff"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")
            df[col] = df[col].interpolate(method="linear", limit=8, limit_direction="forward")
            df[col] = df[col].ffill()

    numeric_cols = df.select_dtypes(include=["number"]).columns.tolist()
    non_numeric_cols = df.select_dtypes(exclude=["number"]).columns.tolist()


    if "timestamp" in numeric_cols:
        numeric_cols.remove("timestamp")
    if "timestamp" in non_numeric_cols:
        non_numeric_cols.remove("timestamp")


    df_numeric = df[["timestamp"] + numeric_cols].copy()
    df_numeric.set_index("timestamp", inplace=True)
    df_numeric = df_numeric.resample("15T").mean().reset_index()


    if non_numeric_cols:
        df_non_numeric = df[["timestamp"] + non_numeric_cols].drop_duplicates("timestamp")
        df = pd.merge(df_numeric, df_non_numeric, on="timestamp", how="left")
    else:
        df = df_numeric


    quality_score = calculate_quality(df)


    report_file = os.path.join(REPORT_DIR, os.path.basename(file_path).replace(".csv", "_quality_report.csv"))
    quality_score.to_csv(report_file, index=True)
    print(f"Data quality report saved to {report_file}")


    cleaned_file_path = os.path.join(OUTPUT_DIR, os.path.basename(file_path))
    df.to_csv(cleaned_file_path, index=False)
    print(f"Cleaned data saved to {cleaned_file_path}")



for energy_file in energy_files:
    process_energy_data(os.path.join(PROCESSED_DIR, energy_file))

print("\nAll files processed successfully.")
