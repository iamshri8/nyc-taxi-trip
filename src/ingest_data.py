import os
import luigi
from luigi.contrib import sqla
import sqlalchemy as sql
from dateutil.parser import parse
import utils as u
import service as s
import pandas as pd
import env


class DownloadData(luigi.Task):
    """ Task for downloading the data. """

    year_month = luigi.MonthParameter()

    def output(self):
        local_path = u.get_local_path(self.year_month)
        return luigi.LocalTarget(local_path)

    def run(self):
        if not os.path.exists(os.getenv("DATA_DIR")):
            os.makedirs(os.getenv("DATA_DIR"))
        local_path = u.download_data(self.year_month)


class CleanData(luigi.Task):
    """ Task for cleaning the dataframe and extracting date. """

    year_month = luigi.MonthParameter()

    def requires(self):
        return DownloadData(self.year_month)

    def run(self):
        df = (
            s.load_data(self.input().path)
            .pipe(s.rename_columns)
            .pipe(s.extract_dates, self.year_month)
        )
        df.to_pickle(self.output().path)

    def output(self):
        return luigi.LocalTarget(
            os.path.join(
                os.getenv("DATA_DIR"),
                f"tmp-{u.year_month_to_str(self.year_month)}.pickle",
            )
        )


class CalculateTripDuration(luigi.Task):
    """ Task for calculating trip duration for each trip. """

    year_month = luigi.MonthParameter()

    def requires(self):
        return CleanData(self.year_month)

    def run(self):
        df = (
            pd.read_pickle(self.input().path)
            .pipe(s.calculate_trip_duration)
            .pipe(s.make_index, column="pickup_time")
        )
        df.to_pickle(self.output().path)

    def output(self):
        return luigi.LocalTarget(
            os.path.join(
                os.getenv("DATA_DIR"),
                f"tmp-{u.year_month_to_str(self.year_month)}.pickle",
            )
        )


class CalculateDailyAverage(luigi.Task):
    """ Task for calculating the average trip duration for each day. """

    year_month = luigi.MonthParameter()

    def requires(self):
        return CalculateTripDuration(self.year_month)

    def run(self):
        df = pd.read_pickle(self.input().path)
        df = s.get_daily_average(df)
        df.to_csv(self.output().path, header=False)

    def output(self):
        return luigi.LocalTarget(
            os.path.join(
                os.getenv("DATA_DIR"),
                f"daily_average_{u.year_month_to_str(self.year_month)}.csv",
            )
        )


class SaveDailyAverage(sqla.CopyToTable):
    """ Task for storing the average trip duration data to the database. """

    year_month = luigi.MonthParameter()

    def requires(self):
        return CalculateDailyAverage(self.year_month)

    columns = [
        (["date", sql.DATE], {"primary_key": True}),
        (["duration", sql.Float], {}),
    ]
    connection_string = os.getenv("CONNECTION_STR_SQL")
    table = "daily_average_duration"
    column_separator = ","

    def rows(self):
        for str_date, daily_avg_trip_duration in super().rows():
            yield parse(str_date).date(), daily_avg_trip_duration


class CalculateRollingAverage(luigi.Task):
    """ Task for calculating the rolling average for 45 days. """

    year_month = luigi.MonthParameter()

    def requires(self):
        return SaveDailyAverage(self.year_month)

    def run(self):
        df = pd.read_sql_table(
            "daily_average_duration",
            con=self.input().engine,
            parse_dates=["date"],
            index_col="date",
        )

        rolling_avg = s.rolling_average_n_days(df, num_of_days=45)
        rolling_avg.to_csv(self.output().path)

    def output(self):
        return luigi.LocalTarget(
            os.path.join(
                os.getenv("DATA_DIR"),
                f"rolling_average_{u.year_month_to_str(self.year_month)}.csv",
            )
        )


class IngestData(luigi.WrapperTask):
    """ Task that starts the data pipeline. """

    year_month = luigi.MonthParameter()

    def requires(self):
        yield CalculateRollingAverage(self.year_month)
