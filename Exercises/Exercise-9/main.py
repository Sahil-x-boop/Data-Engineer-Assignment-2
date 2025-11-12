from data_loader import load_tripdata_lazy
from analytics import rides_per_day, weekly_stats, rides_vs_last_week


def main():
    path = "data/202306-divvy-tripdata.csv"
    lf = load_tripdata_lazy(path)
    
    daily = rides_per_day(lf)
    daily_result = daily.collect()
    print(daily.head())
    
    weekly_result = weekly_stats(daily)
    print(weekly_result.head())
    
    comparison = rides_vs_last_week(daily)
    print(comparison.head())


if __name__ == "__main__":
    main()
