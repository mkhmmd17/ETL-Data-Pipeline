import pandas as pd
import boto3
from io import BytesIO

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
    df.to_csv('clean_data.csv', index=False) # index=False means don't save the DataFrame index as a separate column
    print("Saved clean_data.csv")

def main():
    df = extract_data()
    df = transform_data(df)
    load_data(df)

if __name__ == "__main__":
    main()