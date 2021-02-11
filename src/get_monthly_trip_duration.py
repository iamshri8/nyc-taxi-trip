import os
import service as s
import utils as u
import env


def get_montly_trip_duration(path: str, year_month: str) -> int:
    """ Takes data path and YYYY-MM as input and returns the monthly trip duration."""

    df = (
        s.load_data(path)
        .pipe(s.rename_columns)
        .pipe(s.extract_dates, dt=u.str_to_year_month(year_month))
        .pipe(s.calculate_trip_duration)
        .pipe(s.make_index, column="pickup_time")
        .pipe(s.get_monthly_average)
    )

    return int(df["duration"][0])


if __name__ == "__main__":
    parser = u.arg_parse()
    year_month = u.get_args(parser)

    if not os.path.exists(os.getenv("DATA_DIR")):
        os.makedirs(os.getenv("DATA_DIR"))

    path = u.download_data(u.str_to_year_month(year_month))
    monthly_trip_average = get_montly_trip_duration(path, year_month)
    print(f"Average Trip Duration for {year_month}: ", monthly_trip_average)
