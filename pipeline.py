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

# Step 1: Fetch the data from Kobotoolbox
print("Fetching the Data From Kobotoolbox ...")
response = requests.get(KOBO_CSV_URL, auth=HTTPBasicAuth(KOBO_USERNAME, KOBO_PASSWORD))

if response.status_code == 200:
    print("Data fetched successfully.")

    csv_data = io.StringIO(response.text)
    df = pd.read_csv(csv_data, sep=";", on_bad_lines="skip")
    # Droping the columns because it is generating the missing values
    df.drop(columns='Combat_Intensity', inplace=True, errors='ignore')
    # Changing the data type of columns
    df['Captured'] = df['Captured'].astype('Int64', errors='ignore')
    # Print all column names to check
    print("\n🔍 Available Columns in CSV:")
    print(df.columns.tolist())

    # Step 2: Clean column names
    df.columns = [col.strip().replace(" ", "_").replace("&", "and").replace("-", "_") for col in df.columns]

    print("\n✅ Cleaned Column Names:")
    print(df.columns.tolist())

     #################################################
     # In case we remove this part, we will remove Total_Casualties everywhere too 

    # Try to compute Total_Casualties only if required columns are present
    required_cols = ['Casualties', 'Injured', 'Captured']
    missing = [col for col in required_cols if col not in df.columns]

    if missing:
        print(f"\n⚠️ Missing columns for Total_Casualties calculation: {missing}")
        df['Total_Casualties'] = np.nan
    else:
        df['Total_Casualties'] = df[['Casualties', 'Injured', 'Captured']].sum(axis=1, skipna=True)
    
    ####################################################################

    # Convert 'Date' to datetime
    df['Date'] = pd.to_datetime(df['Date'], errors='coerce')

    # Step 3: Upload to PostgreSQL
    print("Uploading data to PostgreSQL ...")
    conn = psycopg2.connect(
        host=PG_HOST,
        database=PG_DATABASE,
        user=PG_USER,
        password=PG_PASSWORD,
        port=PG_PORT
    )
    cur = conn.cursor()

    # Create schema if not exists
    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name};")

    # Drop and recreate table
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

    # Insert data
    insert_query = f"""
        INSERT INTO {schema_name}.{table_name}(
            "start", "end", "date", Country, Event, Oblast, Casualties, Injured, Captured,
            Civilian_Casualities, New_Recruits, Territory_Status,
            Percentage_Occupied, Area_Occupied, Total_Casualties
        ) VALUES %s
    """

    # # Define the list of expected columns that should exist in the dataframe
    expected = [
        "start", "end", "Date", "Country", "Event", "Oblast", "Casualties", "Injured", "Captured",
        "Civilian_Casualities", "New_Recruits", "Territory_Status",
        "Percentage_Occupied", "Area_Occupied", "Total_Casualties"
    ]
    # Loop through each expected column and check if it exists in the dataframe
    for col in expected:
        # If a column is missing, add it to the dataframe and fill it with None
        if col not in df.columns:
            df[col] = None  # Fill missing with None
    # Prepare the data for insertion into the PostgreSQL database
    # # Extract the rows from the dataframe, but only for the expected columns
    insert_data = df[expected].values.tolist()
    # Insert the data into PostgreSQL using the execute_values function
    # This is an efficient way to insert multiple rows at once
    execute_values(cur, insert_query, insert_data)
    # Commit the transaction to save the changes to the database
    conn.commit()
    # Close the database connection after the operation is complete
    conn.close()
    # Print a success message if the data has been successfully loaded

    print("✅ Data successfully loaded into PostgreSQL.")
else:
    print(f"❌ Failed to fetch data from Kobotoolbox: {response.status_code}")
