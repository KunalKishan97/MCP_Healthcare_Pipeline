import os
import uuid
import json
import pandas as pd
import duckdb
from datetime import datetime

# -----------------------------
# Directory Setup
# -----------------------------
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
DATA_DIR = os.path.join(BASE_DIR, 'data')
CONFIG_DIR = os.path.join(BASE_DIR, 'config')

# -----------------------------
# Load Config Files
# -----------------------------
with open(os.path.join(CONFIG_DIR, 'config.json')) as f:
    config = json.load(f)

with open(os.path.join(CONFIG_DIR, 'mapping.json')) as f:
    mapping = json.load(f)

LOAD_ID = str(uuid.uuid4())
RAW_PATH = os.path.join(DATA_DIR, 'Patient_data_File.csv')
BRONZE_DB = os.path.join(DATA_DIR, 'bronze.duckdb')

# -----------------------------
# Utility Functions
# -----------------------------
def add_audit_columns(df, src_value):
    df["load_id"] = LOAD_ID
    df["load_dtm"] = datetime.now()
    df["src"] = src_value
    return df

def standardize_column_names(df):
    df.columns = [c.strip().lower().replace(" ", "_").replace("-", "_").replace(".", "") for c in df.columns]
    return df

def apply_mapping(df, mapping):
    rename_dict = {v: k for k, v in mapping.items() if v in df.columns}
    df = df.rename(columns=rename_dict)
    return df

def clean_nulls(df):
    df = df.fillna("NA")
    for col in df.columns:
        df[col] = df[col].replace(["", "nan", "NaN", "null", "NULL"], "NA")
    return df

def combine_patient_name(df):
    if "name_first" in df.columns and "name_last" in df.columns:
        df["patientname"] = df["name_first"] + " " + df["name_last"]
    return df


def standardize_dates(df):
    for col in df.columns:
        # Check if column name contains 'date' or 'dob'
        if "date" in col.lower() or "dob" in col.lower():
            try:
                # Attempt to parse as datetime
                df[col] = pd.to_datetime(df[col], errors="coerce")  # convert to datetime
                df[col] = df[col].dt.strftime('%Y-%m-%d')  # format as YYYY-MM-DD
              
            except Exception as e:
                print(f"[WARN] Could not standardize column {col}: {e}")
    return df

def run_ingestion():
    print("[INFO] Starting ingestion process...")
# -----------------------------
# Bronze Layer
# -----------------------------
    print("[INFO] Reading raw data file...")
    df = pd.read_csv(RAW_PATH, skiprows=config['extract'].get('options', {}).get('skiprows', 0), low_memory=False)
    df = standardize_dates(df)
    df = standardize_column_names(df)
    df = add_audit_columns(df, config['src'])
    df = apply_mapping(df, mapping)

    df = clean_nulls(df)

    # Save Bronze table (all columns as STRING to avoid type issues)
    for col in df.columns:
        df[col] = df[col].astype(str)

    con = duckdb.connect(BRONZE_DB)
    con.execute("CREATE TABLE IF NOT EXISTS raw_data AS SELECT * FROM df")
    con.execute("INSERT INTO raw_data SELECT * FROM df")
    con.close()
    print("[INFO] Bronze layer created successfully")


if __name__ == "__main__":
    run_ingestion()