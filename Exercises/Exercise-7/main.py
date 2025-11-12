from spark_session import get_spark_session
from ingestion import read_zipped_csv
from transformation import (
    add_primary_key, add_source_file, add_storage_ranking,
    derive_brand, extract_file_date
)
from reporting import write_report

def main():
    
    spark = get_spark_session()
    
    df = read_zipped_csv(spark, "data")
    print(f"Loaded {df.count()} rows from CSVs")

    df = add_source_file(df)
    df = add_storage_ranking(df)
    df = derive_brand(df)
    df = extract_file_date(df)
    df = add_primary_key(df)

    write_report(df, "reports/final_data")
    spark.stop()

if __name__ == "__main__":
    main()
