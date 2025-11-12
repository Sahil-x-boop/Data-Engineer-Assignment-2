import os

def write_report(df, output_path: str):
    os.makedirs(output_path, exist_ok=True)
    df.write.mode("overwrite").option("header", True).csv(output_path)
    print(f"Report return to {output_path}")

    