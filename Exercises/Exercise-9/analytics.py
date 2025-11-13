import polars as pl

def rides_per_day(lf: pl.LazyFrame) -> pl.LazyFrame:
    return (
        lf
        .with_columns(pl.col("started_at").dt.date().alias("date"))
        .group_by("date")
        .agg(pl.count("ride_id").alias("daily_rides"))
        .sort("date")
    )
    
def weekly_stats(daily: pl.LazyFrame) -> pl.DataFrame:
    df = (
        daily
        .with_columns(pl.col("date").dt.week().alias("week"))
        .group_by("week")
        .agg([
            pl.mean("daily_rides").alias("average_rides"),
            pl.max("daily_rides").alias("max_rides"),
            pl.min("daily_rides").alias("min_rides")
        ])
        .collect()
    )
    return df

def rides_vs_last_week(daily: pl.LazyFrame) -> pl.DataFrame:
    daily_df = daily.collect()
    shifted = daily_df.with_columns([
        (pl.col("daily_rides").shift(7).alias("rides_last_week"))
    ])
    result = shifted.with_columns([
        (pl.col("daily_rides") - pl.col("rides_last_week"))
    ])
    return result
    

