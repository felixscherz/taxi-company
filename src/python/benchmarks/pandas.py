import argparse
import glob
import pandas as pd

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-dir", type=str, required=True, help="Path to parquet data directory")
    args = parser.parse_args()

    files = glob.glob(f"{args.input_dir}/*.parquet")
    df = pd.concat(pd.read_parquet(f) for f in files)

    print("Top pickup locations:")
    print(df.groupby("PULocationID").size().sort_values(ascending=False).head(10))

    df["trip_duration_seconds"] = (df["tpep_dropoff_datetime"] - df["tpep_pickup_datetime"]).dt.total_seconds()
    print("Top 10 longest trip durations:")
    print(df.sort_values("trip_duration_seconds", ascending=False).head(10)[["tpep_pickup_datetime", "tpep_dropoff_datetime", "trip_duration_seconds"]])

    df["ride_date"] = df["tpep_pickup_datetime"].dt.date
    print("Rides per day:")
    print(df.groupby("ride_date").size().sort_values(ascending=False).head(10))

    df["ride_month"] = df["tpep_pickup_datetime"].dt.to_period("M")
    print("Rides per month:")
    print(df.groupby("ride_month").size().sort_values(ascending=False).head(10))


if __name__ == "__main__":
    main()
