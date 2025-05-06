import argparse
import glob
import polars as pl

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-dir", type=str, required=True, help="Path to parquet data directory")
    args = parser.parse_args()

    files = glob.glob(f"{args.input_dir}/*.parquet")
    df = pl.concat([pl.read_parquet(f) for f in files])

    print("Top pickup locations:")
    top_pickup = df.group_by("PULocationID").agg(pl.count().alias("count")).sort("count", descending=True).head(10)
    print(top_pickup)

    df = df.with_columns(
        ((pl.col("tpep_dropoff_datetime").cast(pl.Datetime) - pl.col("tpep_pickup_datetime").cast(pl.Datetime))
         .cast(pl.Int64) / 1e6).alias("trip_duration_seconds")
    )

    print("Top 10 longest trip durations:")
    longest = df.sort("trip_duration_seconds", descending=True).head(10).select(
        ["tpep_pickup_datetime", "tpep_dropoff_datetime", "trip_duration_seconds"]
    )
    print(longest)

    print("Rides per day:")
    rides_day = (
        df.with_columns(pl.col("tpep_pickup_datetime").dt.date().alias("ride_date"))
        .group_by("ride_date")
        .agg(pl.count().alias("count"))
        .sort("count", descending=True)
        .head(10)
    )
    print(rides_day)

    print("Rides per month:")
    rides_month = (
        df.with_columns(pl.col("tpep_pickup_datetime").dt.truncate("1mo").alias("ride_month"))
        .group_by("ride_month")
        .agg(pl.count().alias("count"))
        .sort("count", descending=True)
        .head(10)
    )
    print(rides_month)

if __name__ == "__main__":
    main()
