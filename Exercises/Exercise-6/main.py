# main.py
import os

from spark_session import get_spark_session
from ingestion import read_zipped_csvs
from transformations import (
    clean_and_cast,
    average_trip_duration_per_day,
    trips_per_day,
    most_popular_start_station_monthly,
    top3_stations_last_two_weeks,
    avg_trip_duration_by_gender,
    top10_age_long_short_trips,
)
from reporting import write_report

DATA_DIR = "data"
REPORTS_DIR = "reports"

def main():
    spark = get_spark_session()

    df_raw = read_zipped_csvs(spark, DATA_DIR)

    df = clean_and_cast(df_raw).cache()

    r1 = average_trip_duration_per_day(df)
    r2 = trips_per_day(df)
    r3 = most_popular_start_station_monthly(df)
    r4 = top3_stations_last_two_weeks(df)
    r5 = avg_trip_duration_by_gender(df)
    r6_long, r6_short = top10_age_long_short_trips(df)

    # Write reports
    write_report(r1, REPORTS_DIR, "avg_trip_duration_per_day")
    write_report(r2, REPORTS_DIR, "trips_per_day")
    write_report(r3, REPORTS_DIR, "popular_start_station_monthly")
    write_report(r4, REPORTS_DIR, "top3_stations_last2weeks")
    write_report(r5, REPORTS_DIR, "gender_avg_duration")
    write_report(r6_long, REPORTS_DIR, "top10_age_long_trips")
    write_report(r6_short, REPORTS_DIR, "top10_age_short_trips")


    spark.stop()

if __name__ == "__main__":
    main()
