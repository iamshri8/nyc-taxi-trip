# A simple data pipeline for TLC Trip Record Data.
## Required
- The project requires Anaconda package manager, it can be installed from https://docs.anaconda.com/anaconda/install/

## Installation steps
- $ cd <root_directory>
- $ conda env create -f environment.yml
- $ set PYTHONPATH=%cd%;%PYTHONPATH% (only for windows OS)

###### Note : I have interpreted trip length as the duration of the trip and not as trip distance because the trip distance is already available directly and to calculate the average is straightforward.

## Usage
### Part 1: Python program to calculate the average trip length of all the yellow taxi for a month.
> python get_monthly_trip_duration.py --date 2020-01

Here --date should be in YYYY-MM format.

### Part 2: Data pipeline to ingest new data and calculate the rolling average for 45 days.

> luigi --local-scheduler --module ingest_data IngestData --year-month 2020-01
> luigi --local-scheduler --module ingest_data IngestData --year-month 2020-02
> luigi --local-scheduler --module ingest_data IngestData --year-month 2020-03

[Luigi](https://github.com/spotify/luigi) is used for building the data pipeline t0 ingest new data. The above command is ran three times to ingest data for 3 months and calculate the rolling average for 45 days. In real-time system, cron jobs can be scheduled at the first day of every month to trigger data ingestion and then the rolling average can be calculated.

## Running Unittests
- $ cd <root_directory>
- $ pytest
