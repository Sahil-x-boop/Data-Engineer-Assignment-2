import os
from db_connection import create_table, get_connection
from analytics import (
    load_csv_into_table, popular_vehicle_by_postalcode,
    cars_by_modelyear, cars_per_city, three_most_popular_vehicle
    
)

def main():
    conn = get_connection()
    create_table(conn)
    load_csv_into_table(conn)
    
    print("Daily rides:")
    city_report = cars_per_city(conn)
    print(city_report.head())
    
    print("\nWeekly stats:")
    popular_vehicles = popular_vehicle_by_postalcode(conn)
    print(popular_vehicles.head())

    print("\nRides compared to last week:")
    postalcode_popular = three_most_popular_vehicle(conn)
    print(postalcode_popular.head())
    

    year_report = cars_by_modelyear(conn)

    for year in year_report["model_year"].unique():
        year_df = year_report[year_report["model_year"] == year]

        directory = f"reports/year={year}"
        os.makedirs(directory, exist_ok=True)

        path = f"{directory}/model_year_stats.parquet"
        year_df.to_parquet(path)

print("Analytics complete")


if __name__ == "__main__":
    main()