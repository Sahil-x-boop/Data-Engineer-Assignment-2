from pyspark.sql import SparkSession

def get_spark_session(app_name="Exercise7-PySpark"):
    spark = (
        SparkSession
        .builder
        .appName(app_name)
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("warn")
    return spark