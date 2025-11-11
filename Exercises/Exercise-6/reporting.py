# reporting.py
import os
from pyspark.sql import DataFrame

def write_report(df: DataFrame, reports_dir: str, name: str) -> None:
    os.makedirs(reports_dir, exist_ok=True)
    out_path = os.path.join(reports_dir, name)
    
    (
        df.coalesce(1)  
          .write
          .mode("overwrite")
          .option("header", True)
          .csv(out_path)
    )
