from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from pyspark.sql.window import Window


def add_source_file(df: DataFrame) -> DataFrame:
    return df.withColumn("source_file", input_file_name())

def extract_file_date(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "file_date",
        to_date(
            regexp_extract(col("source_file"), r"(\d{4}-\d{2}-\d{4})",1),
            "yyyy-MM-dd"
        )
    )

def derive_brand(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "brand",
        when(col("model").contains(" "), element_at(split(col("model"), " "), 1))
        .otherwise(lit("unknown"))
    )

def add_storage_ranking(df):
    storage_capacities = Window.partitionBy("model").orderBy(col("capacity_bytes").desc())
    return df.withColumn("storage_ranking", dense_rank().over(storage_capacities))

def add_primary_key(df: DataFrame) -> DataFrame:
    return df.withColumn("primary_key", sha2(concat_ws("-", col("date"), col("serial_number"), col("model")), 256))
