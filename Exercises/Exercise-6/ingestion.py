import os
import tempfile
import zipfile
from typing import List
from pyspark.sql import DataFrame, SparkSession


def _unzipped_files_to_temp(zip_paths: List[str]) -> List[str]:
    tmp_dir = tempfile.mkdtemp(prefix="ex6_unzipped_")
    csv_paths = []
    for z in zip_paths:
        with zipfile.ZipFile(z, "r") as zf:
            for name in zf.namelist():
                if name.lower().endswith(".csv") and "__MACOSX" not in name and not name.startswith("._"):
                    zf.extract(name, tmp_dir)
                    csv_paths.append(os.path.join(tmp_dir, name))
    return csv_paths


def read_zipped_csvs(spark: SparkSession, data_dir: str) -> DataFrame:
    zip_paths = [
        os.path.join(data_dir, f)
        for f in os.listdir(data_dir)
        if f.lower().endswith(".zip")
    ]
    if not zip_paths:
        raise FileNotFoundError(f"No .zip files found in {data_dir}")

    csv_paths = _unzipped_files_to_temp(zip_paths)

    df = None
    for p in csv_paths:
        if "__MACOSX" in p or "._" in p or not p.endswith(".csv"):
            continue
        part = (
            spark.read
            .option("header", True)
            .option("inferSchema", True)
            .csv(p)
        )
        if "start_station_name" in part.columns:
            part = (
                part.withColumnRenamed("start_station_name", "from_station_name")
                .withColumnRenamed("end_station_name", "to_station_name")
            )
        df = part if df is None else df.unionByName(part, allowMissingColumns=True)

    print(f"Total Rows: {df.count() if df else 0}")
    return df
