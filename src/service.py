import pandas as pd
from datetime import datetime


def load_data(path: str) -> pd.DataFrame:
    df = pd.read_csv(
        filepath_or_buffer=path,
        usecols=["tpep_pickup_datetime", "tpep_dropoff_datetime"],
        parse_dates=["tpep_pickup_datetime", "tpep_dropoff_datetime"],
    )

    return df


def rename_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(
        columns={
            "tpep_pickup_datetime": "pickup_time",
            "tpep_dropoff_datetime": "drop_time",
        }
    )

    return df


def extract_dates(df: pd.DataFrame, dt: datetime) -> pd.DataFrame:
    df = df[
        (df["pickup_time"].dt.month == dt.month) & (df["drop_time"].dt.year == dt.year)
    ]

    return df


def calculate_trip_duration(df: pd.DataFrame) -> pd.DataFrame:
    df["duration"] = (df["drop_time"] - df["pickup_time"]).dt.seconds

    return df


def make_index(df: pd.DataFrame, column: str) -> pd.DataFrame:
    return df.set_index(column)


def get_daily_average(df: pd.DataFrame) -> pd.DataFrame:
    return df.resample("D").mean()


def get_monthly_average(df: pd.DataFrame) -> pd.DataFrame:
    return df.resample("M").mean()


def rolling_average_n_days(df: pd.DataFrame, num_of_days: int) -> pd.DataFrame:
    return df.rolling(num_of_days).mean()
