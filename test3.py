from utils import _sanitize_dates
import pandas as pd 
import datetime
a, b = _sanitize_dates(2013, 2014)
print(a)
print(pd.to_datetime(datetime.datetime.today().date()))
end = pd.to_datetime(datetime.datetime.today().date()) + datetime.timedelta(hours=8)
print(end)