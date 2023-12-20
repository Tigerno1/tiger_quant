import os 
from pathlib import Path 
import datetime
# PATH
HOME_PATH = os.path.join(Path.home(), "tiger_quant")
DATA_PATH = os.path.join(HOME_PATH, "data")

# LOG 
LOG_LEVEL = "DEBUG"
LOG_DIR = os.path.join(HOME_PATH, "log")

# CSV PATH 
CSV_PATH = os.path.join(HOME_PATH, "csv")

# SQL 


# HANDLE
NONE_VALUE = ["-", ]


# REQUEST 
CONN_TIMEOUT = 0.3
READ_TIMEOUT = 0.3
PROXY = None

# MULTIPROCESSING 
PRODUCER_NO = 1
CONSUMER_NO = 5


# API 
    # EOD

EOD_TEST = "https://eodhd.com/api/eod/MCD.US?api_token=62516a7aea19f0.48231987"
EOD_BASE_URL= "https://eodhistoricaldata.com/api"
EOD_API_KEY= "62516a7aea19f0.48231987"


EXPIRE_AFTER = datetime.timedelta(days=1)
DEFAULT_INTERVAL = 300 