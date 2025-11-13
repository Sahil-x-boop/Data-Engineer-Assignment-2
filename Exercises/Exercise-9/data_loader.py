import polars as pl

def load_tripdata_lazy(path: str) -> pl.LazyFrame:
    return (
        pl.scan_csv(path)
        .with_columns([
            pl.col("started_at")
                .str.replace('"', "")
                .str.to_datetime("%Y-%m-%d %H:%M:%S", strict=False),
            pl.col("ended_at")
                .str.replace('"', "")
                .str.to_datetime("%Y-%m-%d %H:%M:%S", strict=False),
        ])
        .with_columns([
            pl.col("start_station_id").cast(pl.Float64, strict=False),
            pl.col("end_station_id").cast(pl.Float64, strict=False),
            pl.col("start_lat").cast(pl.Float64, strict=False),
            pl.col("start_lng").cast(pl.Float64, strict=False),
            pl.col("end_lat").cast(pl.Float64, strict=False),
            pl.col("end_lng").cast(pl.Float64, strict=False),
        ])
    )
