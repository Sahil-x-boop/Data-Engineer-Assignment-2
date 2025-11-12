import os
import zipfile
from pyspark.sql import SparkSession

def read_zipped_csv(spark: SparkSession, data_dir: str):
    extracted_files = []

    for file in os.listdir(data_dir):
        if file.endswith(".zip"):
            zip_path = os.path.join(data_dir, file)
            extract_dir = os.path.join(data_dir, "unzipped")
            os.makedirs(extract_dir, exist_ok=True)

            with zipfile.ZipFile(zip_path, "r") as zip_file:
                zip_file.extractall(extract_dir)

            for extracted_file in os.listdir(extract_dir):
                if extracted_file.endswith(".csv"):
                    extracted_files.append(os.path.join(extract_dir, extracted_file))

    df = spark.read.option("header", True).csv(extracted_files)
    return df

'''
Scans the data/ folder for .zip files.

Uses Python’s zipfile.ZipFile to extract them to a temp folder data/unzipped/.

Collects all .csv files inside.

Loads them into Spark using:
'''