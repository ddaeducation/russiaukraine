from httpx import Auth
import pandas as pd
import numpy as np
import psycopg2
import requests
import io 
from requests.auth import HTTPBasicAuth 
from dotenv import load_dotenv
import os

# load environment varialbes
load_dotenv()

# Kobo Credentials

KOBO_USERNAME = os.getenv("KOBO_USERNAME")
KOKO_PASSWORD = os.getenv("KOBO_USERNAME")
KOBO_CSV_URL = "https://kf.kobotoolbox.org/api/v2/assets/aNK3wvPZP3cHZ5uh6goKjj/export-settings/eszJ5XWvpqYT5oJvLWvbuPp/data.csv"

# PostgreSQL credential

PG_HOST = os.getenv("PG_HOST")
PG_PASSWORD = os.getenv("PG_POSSWORD")
PG_DATABASE = os.getenv("PG_POSSWORD")
PG_PORT = os.getenv("PG_HOST")
PG_USER =os.getenv("PG_USER")

# schema and table details
schema_name = "warrecords"
table_name = "ukrainerussia"

# set1: Fetch the data from Kobotoolsbox
print("Fetching the Data From kobotoolsbox .....")
response= requests.get(KOBO_CSV_URL, auth=HTTPBasicAuth(KOBO_USERNAME, KOBO_USERNAME))

if response == 200:
    print("Data Fetched Successfull")

    csv_data = io.StringIO(response.txt)
    df = pd.read_csv(csv_data, sep=",", on_bad_lines="skip")

    # Step 2: Data Cleaning and Trnansformation
    print("Data Processing .....")
    df.columns=[col.strip().replace(" ","_").replace("&", "and").replace("-","_") for col in df.columns] 

    # Step 3: Computing the new columns
    df['Total_Casualties'] = df["Casualties","Injured",'Captured'].sum(axis=1)

    # Convert the Data to data format
    df['Date']=pd.to_datetime(df['Date'], errors='coerce')

    # Step 4: Uploading to the PostgreSQL
    print("Uploading data to postgreSQL")

    conn= psycopg2.connect(
        host = PG_HOST,
        database = PG_DATABASE,
        user = PG_USER,
        password = PG_PASSWORD,
        port = PG_PORT
    )

    cur = conn.cursor()
    # Create the schema if it doesn't exit 
    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name};")

    # Drop and Recreate the table
    cur.execute(f"DROP TABLE IF EXISTS {schema_name}.{table_name};")
    cur.execute(f"""
        CREATE TABLE {schema_name}.{table_name}(
            id SERIAL PRIMARY KEY,
            "star" TIMESTAMP,
            "end" TIMESTAMP,
            "date" Date,
            Country TEXT,
            Event TEX,
            Oblast TEXT,
            Casualties INT,
            Injured INT,
            Captured INT,
            Civilian_Casualities INT,
            New_Recruits INT,
            Cambat_Intensity FLOAT,
            Territory_Status TEXT,
            Percentage_Occupied FLOAT,
            Area_Occupied INT,
            Total_Casualties
        );
    """)

    # Insert data row by row in 
    insert_query=f"""
        INSERT INTO {schema_name}.{table_name}(
            "star","end","date",Country,Event,Oblast,Casualties,Injured,Captured,
            Civilian_Casualities,New_Recruits,Cambat_Intensity, Territory_Status, 
            Percentage_Occupied,Area_Occupied,Total_Casualties
        ) VALUES (%s, %s,%s, %s,%s, %s,%s, %s,%s, %s,%s, %s,%s, %s,%s, %s,%s, %s,%s, %s)
    """

    for _, row in df.iterrows():
        cur.execute(insert_query,(
            row.get("start"),
            row.get("end"),
            row.get("date"),
            row.get("Country"),
            row.get("Event"),
            row.get("Oblast"),
            row.get("Casualties"),
            row.get("Injured"),
            row.get("Captured"),
            row.get("Civilian_Casualities"),
            row.get("New_Recruits"),
            row.get("Cambat_Intensity"),
            row.get("Territory_Status"),
            row.get("Percentage_Occupied"),
            row.get("Area_Occupied"),
            row.get("Total_Casualties")
        ))
    conn.commit()
    conn.close()

    print("Data Successfully loaded in the Postgresql")
else:
    print(f"Failed to fetch data from kobotoolbox: {response.status_code}")






