import duckdb

def get_connection():
    conn = duckdb.connect("electric_cars.duckdb")
    return conn

def create_table(conn):
    conn.execute("""
    CREATE TABLE IF NOT EXISTS electric_cars (
        vin VARCHAR,
        county VARCHAR,
        city VARCHAR,
        state VARCHAR,
        postal_code VARCHAR,
        model_year INTEGER,
        make VARCHAR,
        model VARCHAR,
        ev_type VARCHAR,
        cafv_eligibility VARCHAR,
        electric_range INTEGER,
        base_msrp INTEGER,
        legislative_district INTEGER,
        dol_vehicle_id BIGINT,
        vehicle_location VARCHAR,
        electric_utility VARCHAR,
        census_tract BIGINT
    );
    """)
