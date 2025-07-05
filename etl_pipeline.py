import pandas as pd

def extract_data():
    df = pd.read_parquet('yellow_tripdata_2024-01.parquet')
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