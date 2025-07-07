import pandas as pd
import boto3
from io import BytesIO
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine

def extract_data():
    bucket = "nyc-etl-demo-mgelogaev"
    key = "yellow_tripdata_2024-01.parquet"

    s3 = boto3.client("s3")
    obj = s3.get_object(Bucket=bucket, Key=key)
    data = obj["Body"].read()
    df = pd.read_parquet(BytesIO(data))
    return df
    

def transform_data(df):
    df['trip_duration'] = (df['tpep_dropoff_datetime'] - df['tpep_pickup_datetime']).dt.total_seconds()
    return df[['tpep_pickup_datetime', 'tpep_dropoff_datetime', 'trip_distance', 'trip_duration']]

def load_data(df):
    load_dotenv()

    df.to_csv('clean_data.csv', index=False)
    print("Saved clean_data.csv")

    user = os.getenv("DB_USER")
    pwd = os.getenv("DB_PASS")
    host = os.getenv("DB_HOST")
    port = os.getenv("DB_PORT")
    name = os.getenv("DB_NAME")

    uri = f"postgresql://{user}:{pwd}@{host}:{port}/{name}"

    engine = create_engine(uri)

    df.to_sql("taxi_trips", engine, if_exists="replace", index=False)

    print("Uploaded data to RDS table taxi_trips")

def main():
    df = extract_data()
    df = transform_data(df)
    load_data(df)

if __name__ == "__main__":
    main()