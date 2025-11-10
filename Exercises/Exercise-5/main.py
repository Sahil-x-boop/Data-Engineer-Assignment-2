import psycopg2
from db_connection import connect_db
from db_schema import create_tables
from data_loader import insert_csv_data

def main():

    conn = connect_db()
    create_tables(conn)

    insert_csv_data(conn, "data/accounts.csv", "accounts")
    insert_csv_data(conn, "data/products.csv", "products")
    insert_csv_data(conn, "data/transactions.csv", "transactions")

    conn.close()
    print("All data loaded successfully into PostgreSQL!")

if __name__ == "__main__":
    main()
