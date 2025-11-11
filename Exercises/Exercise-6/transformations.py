

from pyspark.sql import DataFrame, functions as F, Window

def clean_and_cast(df: DataFrame) -> DataFrame:
    for col in ["start_time", "end_time", "tripduration", "from_station_name", "gender", "birthyear"]:
        if col not in df.columns:
            for alt in [col.upper(), col.title(), col.replace("_", " ")]:
                if alt in df.columns:
                    df = df.withColumnRenamed(alt, col)
                    break
    df = (
        df
        .withColumn("start_time", F.to_timestamp(F.col("start_time")))
        .withColumn("end_time", F.to_timestamp(F.col("end_time")))
        .withColumn("tripduration", F.col("tripduration").cast("double"))
        .withColumn("birthyear", F.col("birthyear").cast("int"))
        .withColumn("gender", F.trim(F.lower(F.col("gender")))) 
    )
    return df

def average_trip_duration_per_day(df: DataFrame) -> DataFrame:
    return (
        df
        .withColumn("day", F.to_date("start_time"))
        .groupBy("day")
        .agg(F.avg("tripduration").alias("avg_tripduration"))
        .orderBy("day")
    )
    

def trips_per_day(df: DataFrame) -> DataFrame:
    return (
        df
        .withColumn("day", F.to_date("start_time"))
        .groupby("day")
        .agg(F.count("trip_id").alias("trip_count"))
        .orderBy("day")
    )

def most_popular_start_station_monthly(df: DataFrame) -> DataFrame:
    base = (
        df
        .withColumn("month", F.date_trunc("month", F.col("start_time")).cast("date"))
        .groupBy("month", "from_station_name")
        .agg(F.count(F.lit(1)).alias("trips"))
    )
    w =  Window.partitionBy("month").orderBy(F.col("trips").desc(), F.col("from_station_name").asc())
    return (
        base
        .withColumn("rnk", F.row_number().over(w))
        .where(F.col("rnk") == 1)
        .drop("rnk")
        .orderBy("month")
    )

def top3_stations_last_two_weeks(df):
    
    df = df.withColumn("day", F.to_date("start_time"))
    
    last_day = df.agg(F.max("day")).collect()[0][0]
    two_weeks_ago = F.date_sub(F.lit(last_day), 13)
    
    recent_df = df.filter((F.col("day") >= two_weeks_ago) & (F.col("day") <= F.lit(last_day)))
    
    w = Window.partitionBy("day").orderBy(F.desc("trips"), F.asc("from_station_name"))
    
    result = (recent_df.groupBy("day", "from_station_name")
              .agg(F.count("*").alias("trips"))
              .withColumn("rnk", F.row_number().over(w))
              .filter(F.col("rnk") <= 3))
    
    return result

def avg_trip_duration_by_gender(df: DataFrame) -> DataFrame:
    return (
        df
        .where(F.col("gender").isNotNull() & (F.col("gender") != ""))
        .groupBy("gender")
        .agg(F.avg("tripduration").alias("avg_tripduration"))
        .orderBy("avg_tripduration")
    )

def top10_age_long_short_trips(df):
    df = df.withColumn("age", F.year(F.to_date("start_time")) - F.col("birthyear"))
    df = df.filter(
        (F.col("age").isNotNull()) &
        (F.col("age") > 0) &
        (F.col("age") < 120)
    )

    agg = df.groupBy("age").agg(F.avg("tripduration").alias("avg_tripduration"))

    top_long = (
        agg.orderBy(F.col("avg_tripduration").desc(), F.col("age").desc())
        .limit(10)
    )

    top_short = (
        agg.orderBy(F.col("avg_tripduration").asc(), F.col("age").asc())
        .limit(10)
    )

    return top_long, top_short