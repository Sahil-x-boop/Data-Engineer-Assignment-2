import psycopg2
import os
import time

DB_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "database": os.getenv("POSTGRES_DB", "exercise5"),
    "user": os.getenv("POSTGRES_USER", "postgres"),
    "password": os.getenv("POSTGRES_PASSWORD", "postgres"),
    "port": os.getenv("POSTGRES_PORT", 5433)
}

def connect_db():
    for attempt in range(10):
        try:
            conn = psycopg2.connect(**DB_CONFIG)
            print("Connected to PostgreSQL")
            return conn
        except psycopg2.OperationalError as e:
            print(f"Attempt {attempt+1}: Database not ready, retrying...")
            time.sleep(3)
    raise Exception("Could not connect")
