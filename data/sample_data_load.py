#!/usr/bin/env python3
"""
YouTube Analytics Project - Sample Data Loader

This script downloads the YouTube Video Statistics dataset from Kaggle
and uploads it to Snowflake.

Requirements:
- kaggle API credentials (kaggle.json)
- snowflake-connector-python
- pandas
"""

import os
import json
import subprocess
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import zipfile
import glob

# Configuration
KAGGLE_DATASET = "datasnaek/youtube-new"
DOWNLOAD_DIR = "youtube_data"
SNOWFLAKE_CONFIG = {
    "account": "<your_account>",
    "user": "<your_username>",
    "password": "<your_password>",
    "warehouse": "YOUTUBE_LOAD_WH",
    "database": "YOUTUBE_ANALYTICS",
    "schema": "RAW"
}

def setup_directories():
    """Create directories for downloaded data"""
    if not os.path.exists(DOWNLOAD_DIR):
        os.makedirs(DOWNLOAD_DIR)
    print(f"Created directory: {DOWNLOAD_DIR}")

def download_kaggle_dataset():
    """Download the dataset from Kaggle using the Kaggle API"""
    try:
        print(f"Downloading dataset {KAGGLE_DATASET}...")
        subprocess.run(["kaggle", "datasets", "download", KAGGLE_DATASET, "-p", DOWNLOAD_DIR], check=True)
        
        # Extract the zip file
        zip_files = glob.glob(f"{DOWNLOAD_DIR}/*.zip")
        for zip_file in zip_files:
            with zipfile.ZipFile(zip_file, 'r') as zip_ref:
                zip_ref.extractall(DOWNLOAD_DIR)
            print(f"Extracted {zip_file}")
        
        print("Dataset downloaded and extracted successfully")
    except Exception as e:
        print(f"Error downloading dataset: {e}")
        raise

def connect_to_snowflake():
    """Connect to Snowflake and return connection"""
    try:
        conn = snowflake.connector.connect(
            user=SNOWFLAKE_CONFIG["user"],
            password=SNOWFLAKE_CONFIG["password"],
            account=SNOWFLAKE_CONFIG["account"],
            warehouse=SNOWFLAKE_CONFIG["warehouse"],
            database=SNOWFLAKE_CONFIG["database"],
            schema=SNOWFLAKE_CONFIG["schema"]
        )
        print("Connected to Snowflake")
        return conn
    except Exception as e:
        print(f"Error connecting to Snowflake: {e}")
        raise

def upload_csv_files(conn):
    """Upload CSV files to Snowflake"""
    try:
        csv_files = glob.glob(f"{DOWNLOAD_DIR}/*.csv")
        
        for csv_file in csv_files:
            file_name = os.path.basename(csv_file)
            print(f"Processing {file_name}...")
            
            # Read CSV file
            df = pd.read_csv(csv_file)
            
            # Add source file column
            df['source_file'] = file_name
            
            # Write to Snowflake
            success, num_chunks, num_rows, output = write_pandas(
                conn, 
                df, 
                'VIDEOS',
                auto_create_table=False
            )
            
            print(f"Uploaded {file_name}: {num_rows} rows")
    except Exception as e:
        print(f"Error uploading CSV files: {e}")
        raise

def upload_json_files(conn):
    """Upload JSON files to Snowflake"""
    try:
        json_files = glob.glob(f"{DOWNLOAD_DIR}/*.json")
        cursor = conn.cursor()
        
        for json_file in json_files:
            file_name = os.path.basename(json_file)
            print(f"Processing {file_name}...")
            
            # Read JSON file
            with open(json_file, 'r') as f:
                json_data = json.load(f)
            
            # Convert to string
            json_str = json.dumps(json_data)
            
            # Insert into Snowflake
            cursor.execute(
                "INSERT INTO CATEGORIES (raw_json, source_file) VALUES (%s, %s)",
                (json_str, file_name)
            )
            
            print(f"Uploaded {file_name}")
    except Exception as e:
        print(f"Error uploading JSON files: {e}")
        raise

def main():
    """Main function to orchestrate the data loading process"""
    try:
        setup_directories()
        download_kaggle_dataset()
        
        conn = connect_to_snowflake()
        upload_csv_files(conn)
        upload_json_files(conn)
        
        # Close connection
        conn.close()
        print("Data loading completed successfully")
    except Exception as e:
        print(f"Error in data loading process: {e}")

if __name__ == "__main__":
    main()
