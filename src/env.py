import os
from pathlib import Path

os.environ["DATA_DIR"] = os.path.join(Path(os.getcwd()).parent, "data")
os.environ["CONNECTION_STR_SQL"] = "sqlite:///daily_average.sqlite"
os.environ["BASE_URL"] = "https://s3.amazonaws.com/nyc-tlc/trip+data/"
