from duckdb import connect
import argparse


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-dir", type=str, required=True, help="Path to parquet data directory")
    args = parser.parse_args()
    db = connect()

    db.read_parquet(f"{args.input_dir}/*.parquet").create("df")

    # print(db.sql("DESCRIBE df"))

    # ┌───────────────────────┬─────────────┬─────────┬─────────┬─────────┬─────────┐
    # │      column_name      │ column_type │  null   │   key   │ default │  extra  │
    # │        varchar        │   varchar   │ varchar │ varchar │ varchar │ varchar │
    # ├───────────────────────┼─────────────┼─────────┼─────────┼─────────┼─────────┤
    # │ VendorID              │ INTEGER     │ YES     │ NULL    │ NULL    │ NULL    │
    # │ tpep_pickup_datetime  │ TIMESTAMP   │ YES     │ NULL    │ NULL    │ NULL    │
    # │ tpep_dropoff_datetime │ TIMESTAMP   │ YES     │ NULL    │ NULL    │ NULL    │
    # │ passenger_count       │ BIGINT      │ YES     │ NULL    │ NULL    │ NULL    │
    # │ trip_distance         │ DOUBLE      │ YES     │ NULL    │ NULL    │ NULL    │
    # │ RatecodeID            │ BIGINT      │ YES     │ NULL    │ NULL    │ NULL    │
    # │ store_and_fwd_flag    │ VARCHAR     │ YES     │ NULL    │ NULL    │ NULL    │
    # │ PULocationID          │ INTEGER     │ YES     │ NULL    │ NULL    │ NULL    │
    # │ DOLocationID          │ INTEGER     │ YES     │ NULL    │ NULL    │ NULL    │
    # │ payment_type          │ BIGINT      │ YES     │ NULL    │ NULL    │ NULL    │
    # │ fare_amount           │ DOUBLE      │ YES     │ NULL    │ NULL    │ NULL    │
    # │ extra                 │ DOUBLE      │ YES     │ NULL    │ NULL    │ NULL    │
    # │ mta_tax               │ DOUBLE      │ YES     │ NULL    │ NULL    │ NULL    │
    # │ tip_amount            │ DOUBLE      │ YES     │ NULL    │ NULL    │ NULL    │
    # │ tolls_amount          │ DOUBLE      │ YES     │ NULL    │ NULL    │ NULL    │
    # │ improvement_surcharge │ DOUBLE      │ YES     │ NULL    │ NULL    │ NULL    │
    # │ total_amount          │ DOUBLE      │ YES     │ NULL    │ NULL    │ NULL    │
    # │ congestion_surcharge  │ DOUBLE      │ YES     │ NULL    │ NULL    │ NULL    │
    # │ Airport_fee           │ DOUBLE      │ YES     │ NULL    │ NULL    │ NULL    │
    # ├───────────────────────┴─────────────┴─────────┴─────────┴─────────┴─────────┤
    # │ 19 rows                                                           6 columns │
    # └─────────────────────────────────────────────────────────────────────────────┘

    # average fare

    # print(db.sql("SELECT avg(fare_amount) FROM df").to_df().describe())
    print(db.sql("SELECT * FROM df").to_df().describe())

    # top pickup locations

    print(
        db.sql(
            "SELECT PULocationID, COUNT(*) as cnt FROM df GROUP BY PULocationID ORDER BY cnt DESC LIMIT 10"
        )
    )

    # pickup durations

    print(
        db.sql(
            "SELECT tpep_pickup_datetime, tpep_dropoff_datetime, EXTRACT(EPOCH FROM (tpep_dropoff_datetime - tpep_pickup_datetime)) as trip_duration_seconds FROM df ORDER BY trip_duration_seconds DESC LIMIT 10"
        ).to_df()
    )


    # rides per day
    print(
        db.sql(
            "SELECT CAST(tpep_pickup_datetime AS DATE) AS ride_date, COUNT(*) as cnt FROM df GROUP BY ride_date ORDER BY cnt DESC LIMIT 10"
        ).to_df()
    )

    # rides per month
    print(
        db.sql(
            "SELECT DATE_TRUNC('month', tpep_pickup_datetime) AS ride_month, COUNT(*) as cnt FROM df GROUP BY ride_month ORDER BY cnt DESC LIMIT 10"
        ).to_df()
    )

if __name__ == "__main__":
    main()
