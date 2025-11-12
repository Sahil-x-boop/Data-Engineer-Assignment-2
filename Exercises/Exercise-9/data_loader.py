import polars as pl

def load_tripdata_lazy(path: str) -> pl.LazyFrame:
    df = (
        pl.scan_csv(path, has_header=True)
        .with_columns([
            pl.col("started_at").str.strptime(pl.Datetime, format="%Y-%m-%d %H-%M-%s"),
            pl.col("ended_at").str.strptime(pl.Datetime, format="%Y-%m-%d %H-%M-%S"),
            pl.col("start_station_id").cast(pl.Float64),
            pl.col("end_station_id").cast(pl.Float64),
            pl.col("start_lng").cast(pl.Float64),
            pl.col("start_lat").cast(pl.Float64),
            pl.col("end_lat").cast(pl.Float64),
            pl.col("end_lng").cast(pl.Float64),
        ])
    )
    return df