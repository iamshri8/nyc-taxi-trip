import os
import sys

sys.path.append(os.path.join(os.getcwd(), "src"))
import pytest
from datetime import datetime
import pandas as pd
import numpy as np
import service as s


@pytest.fixture
def mock_data():
    return pd.DataFrame(
        data={
            "pickup_time": [
                datetime(2021, 1, 1, 8, 0),
                datetime(2021, 2, 1, 9, 30),
                datetime(2021, 1, 1, 10, 0),
            ],
            "drop_time": [
                datetime(2021, 1, 1, 8, 30),
                datetime(2021, 2, 1, 10, 0),
                datetime(2021, 1, 1, 11, 0),
            ],
        },
        columns=["pickup_time", "drop_time"],
    )


def test_extract_dates(mock_data):
    assert len(s.extract_dates(mock_data, datetime(2021, 1, 1))) == 2
    assert len(s.extract_dates(mock_data, datetime(2021, 2, 1))) == 1
    assert len(s.extract_dates(mock_data, datetime(2021, 2, 1))) != 2


def test_calculate_trip_duration(mock_data):
    expected_trip_duration = [1800, 1800, 3600]  # All values are in seconds.
    actual_trip_duration = s.calculate_trip_duration(mock_data)
    assert all(actual_trip_duration["duration"] == expected_trip_duration)


def test_rename_columns():
    df = pd.DataFrame(columns=["tpep_pickup_datetime", "tpep_dropoff_datetime"])
    expected_cols = pd.DataFrame(columns=["pickup_time", "drop_time"])
    actual_cols = s.rename_columns(df).columns
    assert all(actual_cols == expected_cols)


def test_get_monthly_average():
    df = pd.DataFrame(
        data={"duration": [2] * 30},
        columns=["duration"],
        index=pd.date_range(start="2020-04-01", freq="1D", periods=30),
    )
    expected_avg = 2
    actual_avg = s.get_monthly_average(df)["duration"]
    assert all(expected_avg == actual_avg)


def test_get_daily_average():
    df = pd.DataFrame(
        data={"duration": [1, 3, 4, 4] * 30},
        columns=["duration"],
        index=pd.date_range(start="2020-04-01", freq="6H", periods=120),
    )
    expected_avg = [3] * 30
    actual_avg = s.get_daily_average(df)["duration"]
    assert all(expected_avg == actual_avg)


def test_rolling_average_n_days():
    df = pd.DataFrame(
        data={"duration": [1] * 60},
        columns=["duration"],
        index=pd.date_range(start="2021-01-01", freq="1D", periods=60),
    )
    expected_rolling_avg = pd.DataFrame(
        data={"duration": [np.nan] * 44 + [1] * 16},
        columns=["duration"],
        index=pd.date_range(start="2021-01-01", freq="1D", periods=60),
    )
    actual_rolling_avg = s.rolling_average_n_days(df, 45)
    pd.testing.assert_frame_equal(expected_rolling_avg, actual_rolling_avg)
