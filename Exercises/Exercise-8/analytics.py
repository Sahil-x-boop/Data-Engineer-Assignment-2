def load_csv_into_table(conn):
    conn.execute("""
        CREATE OR REPLACE TABLE electric_cars AS
        SELECT *
        FROM read_csv_auto('data/Electric_Vehicle_Population_Data.csv', HEADER := TRUE);
    """)
    conn.execute("""
        ALTER TABLE electric_cars RENAME COLUMN "Postal Code" TO postal_code;
    """)
    conn.execute("""
        ALTER TABLE electric_cars RENAME COLUMN "Model Year" TO model_year;
    """)


def cars_per_city(conn):
    return conn.execute("""
        select city, count(*) as total_count
        from electric_cars
        group by city
        order by total_count desc;
    """).fetchdf()

def three_most_popular_vehicle(conn):
    return conn.execute("""
        select Make, Model, count(*) as total_count
        from electric_cars
        group by Make, Model
        order by count(Model) desc
        limit 3;
    """).fetchdf()

def popular_vehicle_by_postalcode(conn):
    return conn.execute("""
        with portal_wise as 
        (
        SELECT Postal_Code, Make, Model, COUNT(*) AS counts,
            ROW_NUMBER() OVER (PARTITION BY Postal_Code ORDER BY count(*) desc) as rnk
        FROM electric_cars
        GROUP BY Postal_Code, Make, Model
        )
        select Postal_code, Make, Model, counts
        from portal_wise
        where rnk = 1
        order by Postal_code desc;
    """).fetchdf()

def cars_by_modelyear(conn):
    return conn.execute("""
        select Model_Year, count(*) as total_count
        from electric_cars
        group by Model_Year
        order by total_count desc;
    """).fetchdf()

