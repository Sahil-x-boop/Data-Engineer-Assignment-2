import pandas as pd

def insert_csv_data(conn, csv_path, table_name):
    df = pd.read_csv(csv_path)

    df.columns = [col.strip().lower() for col in df.columns]

    with conn.cursor() as cur:
        for _, row in df.iterrows():
            columns = ', '.join(df.columns)
            placeholders = ', '.join(['%s'] * len(df.columns))
            sql = f"insert into {table_name} ({columns}) values ({placeholders}) on conflict do nothing"
            cur.execute(sql, tuple(row))
        conn.commit()
    
    print(f"inserted successfully into {table_name}.")