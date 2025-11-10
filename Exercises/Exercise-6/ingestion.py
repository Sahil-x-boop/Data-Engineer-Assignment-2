import os
import tempfile
import zipfile
from typing import List

from pyspark.sql import DataFrame, SparkSession

def _unzip_files_to_temp(zip_paths: List[str]) -> List[str]:
    tmp_dir = tempfile.mkdtemp(prefix="ex6_unzipped_")
    csv_paths = []
    for z in zip_paths:
        with zipfile.ZipFile(z, "r") as zf:
            for name in zf.namelist():
                if name.lower().endswith(".csv"):
                    zf.extract(name, tmp_dir)
                    csv_paths.append(os.path.join(tmp_dir, name))
    return csv_paths

def read_zipped_csvs(spark: SparkSession, data_dir: str) -> DataFrame:
    zip_paths = [
        os.path.join(data_dir, f)
        for f in os.listdir(data_dir)
        if f.lower().endswith(".zip")
    ]