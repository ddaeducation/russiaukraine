import pandas as pd
import numpy as np
import psycopg2
from psycopg2.extras import execute_values
import requests
import io
from requests.auth import HTTPBasicAuth
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

# Kobo Credentials
KOBO_USERNAME = os.getenv("KOBO_USERNAME")
KOBO_PASSWORD = os.getenv("KOBO_PASSWORD")
KOBO_CSV_URL = "https://kf.kobotoolbox.org/api/v2/assets/aNK3wvPZP3cHZ5uh6goKjj/export-settings/eszJ5XWvpqYT5oJvLWvbuPp/data.csv"

# PostgreSQL Credentials
PG_HOST = os.getenv("PG_HOST")
PG_PASSWORD = os.getenv("PG_PASSWORD")
PG_DATABASE = os.getenv("PG_DATABASE")
PG_PORT = os.getenv("PG_PORT")
PG_USER = os.getenv("PG_USER")

# Schema and table details
schema_name = "warrecords"
table_name = "ukrainerussia"

print("Fetching the Data From Kobotoolbox ...")
response = requests.get(KOBO_CSV_URL, auth=HTTPBasicAuth(KOBO_USERNAME, KOBO_PASSWORD))

if response.status_code == 200:
    print("Data fetched successfully.")

    csv_data = io.StringIO(response.text)
    df = pd.read_csv(csv_data, sep=";", on_bad_lines="skip")

    # Drop problematic columns if they exist
    df.drop(columns='Cambat Intensity', inplace=True, errors='ignore')

    # Change types
    if 'Captured' in df.columns:
        df['Captured'] = pd.to_numeric(df['Captured'], errors='coerce').astype('Int64')

    print("\n🔍 Available Columns in CSV:")
    print(df.columns.tolist())

    # Clean column names
    df.columns = [col.strip().replace(" ", "_").replace("&", "and").replace("-", "_") for col in df.columns]

    print("\n✅ Cleaned Column Names:")
    print(df.columns.tolist())

     ##############################
    # Compute Total_Casualties if possible
    required_cols = ['Casualties', 'Injured', 'Captured']
    if all(col in df.columns for col in required_cols):
        df['Total_Casualties'] = df[required_cols].sum(axis=1, skipna=True)
    else:
        df['Total_Casualties'] = np.nan
        print(f"\n⚠️ Missing columns for Total_Casualties calculation: {[col for col in required_cols if col not in df.columns]}")
    ####################################
    
    # Convert date
    df['Date'] = pd.to_datetime(df['Date'], errors='coerce')

    print("Uploading data to PostgreSQL ...")
    conn = psycopg2.connect(
        host=PG_HOST,
        database=PG_DATABASE,
        user=PG_USER,
        password=PG_PASSWORD,
        port=PG_PORT
    )
    cur = conn.cursor()

    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name};")
    cur.execute(f"DROP TABLE IF EXISTS {schema_name}.{table_name};")
    cur.execute(f"""
        CREATE TABLE {schema_name}.{table_name}(
            id SERIAL PRIMARY KEY,
            "start" TIMESTAMP,
            "end" TIMESTAMP,
            "date" DATE,
            Country TEXT,
            Event TEXT,
            Oblast TEXT,
            Casualties INT,
            Injured INT,
            Captured INT,
            Civilian_Casualities INT,
            New_Recruits INT,
            Territory_Status TEXT,
            Percentage_Occupied FLOAT,
            Area_Occupied INT,
            Total_Casualties INT
        );
    """)

    expected = [
        "start", "end", "Date", "Country", "Event", "Oblast", "Casualties", "Injured", "Captured",
        "Civilian_Casualities", "New_Recruits", "Territory_Status",
        "Percentage_Occupied", "Area_Occupied", "Total_Casualties"
    ]
    for col in expected:
        if col not in df.columns:
            df[col] = None

    # Convert to Python native types
    df = df[expected].astype(object).where(pd.notnull(df), None)

    insert_data = df.values.tolist()
    insert_query = f"""
        INSERT INTO {schema_name}.{table_name}(
            "start", "end", "date", Country, Event, Oblast, Casualties, Injured, Captured,
            Civilian_Casualities, New_Recruits, Territory_Status,
            Percentage_Occupied, Area_Occupied, Total_Casualties
        ) VALUES %s
    """

    execute_values(cur, insert_query, insert_data)
    conn.commit()
    conn.close()
    print("✅ Data successfully loaded into PostgreSQL.")
else:
    print(f"❌ Failed to fetch data from Kobotoolbox: {response.status_code}")
