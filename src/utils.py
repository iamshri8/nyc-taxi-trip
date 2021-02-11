import os
import sys
from datetime import datetime
from pathlib import Path
import argparse
import requests
import env
from tqdm import tqdm


def arg_parse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", help="Year and Month in yyyy-mm.", required=True)

    return parser


def get_args(parser: argparse.ArgumentParser) -> str:
    args = parser.parse_args()
    year_month = args.date

    return year_month


def get_url(year_month: datetime) -> str:
    full_url = "".join([os.getenv("BASE_URL"), get_file_name(year_month)])

    return full_url


def year_month_to_str(year_month: datetime) -> str:
    return f"{year_month.year}-{year_month.month:02d}"


def str_to_year_month(year_month: str) -> datetime:
    return datetime.strptime(year_month, "%Y-%m")


def get_file_name(year_month: datetime, format="csv") -> str:
    return f"yellow_tripdata_{year_month_to_str(year_month)}.{format}"


def get_local_path(year_month: datetime, format="csv") -> str:
    local_path = os.path.join(
        os.getenv("DATA_DIR"), get_file_name(year_month=year_month, format=format)
    )

    return local_path


def download_data(year_month: datetime) -> str:
    chunk_size = 1024
    url = get_url(year_month)
    local_path = get_local_path(year_month, "csv")

    if os.path.exists(local_path):
        return local_path

    filesize = int(requests.head(url).headers["Content-Length"])
    progress_bar = tqdm(
        unit="B",
        unit_scale=True,
        unit_divisor=1024,
        total=filesize,
        file=sys.stdout,
        desc=local_path,
    )
    with requests.get(url, stream=True) as response:
        response.raise_for_status()
        with open(local_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=chunk_size):
                if chunk:
                    datasize = f.write(chunk)
                    progress_bar.update(datasize)
    progress_bar.close()

    return local_path
