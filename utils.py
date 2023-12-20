import numpy as np
import pandas as pd
import importlib
import pandas.api.types as _types
from typing import Union, Tuple, Optional
from functools import wraps
from datetime import date, datetime, timedelta
from sqlite3 import Timestamp
from dateutil import parser as date_parser

from conf import DEFAULT_INTERVAL

SanitizeType = Tuple[Union[Timestamp, datetime], Union[Timestamp, datetime]]

def gt(
    a: float | pd.Series, b: float | pd.Series, precision: float = 1e-4
) -> bool | pd.Series:
    return (a - b) > precision


def lt(
    a: float | pd.Series, b: float | pd.Series, precision: float = 1e-4
) -> bool | pd.Series:
    return (b - a) > precision


def eq(
    a: float | pd.Series, b: float | pd.Series, precision: float = 1e-4
) -> bool | pd.Series:
    if isinstance(a, float) and isinstance(b, float):
        return abs(a - b) < precision
    elif isinstance(a, pd.Series) and isinstance(b, pd.Series):
        return (a - b).abs() < precision
    else:
        raise TypeError("a and b must be both float or both pd.Series")


def gte(
    a: float | pd.Series, b: float | pd.Series, precision: float = 1e-4
) -> bool | pd.Series:
    return gt(a, b, precision) | eq(a, b, precision)


def lte(
    a: float | pd.Series, b: float | pd.Series, precision: float = 1e-4
) -> bool | pd.Series:
    return lt(a, b, precision) | eq(a, b, precision)


def between(
    a: float | pd.Series,
    lower: float | pd.Series,
    upper: float | pd.Series,
    precision: float = 1e-4,
) -> bool | pd.Series:
    return gte(a, lower, precision) & lte(a, upper, precision)

def chunks(lst, batch_size: int):
    for i in range(0, len(lst), batch_size):
        yield lst[i : i + batch_size]


def calc_pct_chg(base_price, then_price) -> float:
    res = 100 * (then_price - base_price) / base_price
    return round(res, 2)


def calc_days_diff(d1: date | str, d2: date | str) -> int:
    if isinstance(d1, str):
        d1 = date_parser.parse(d1).date()

    if isinstance(d2, str):
        d2 = date_parser.parse(d2).date()

    return (d1 - d2).days


def round_val(fun):
    @wraps(fun)
    def inner(*args, **kwargs):
        val = fun(*args, **kwargs)
        return round(val, 2)

    return inner


def optimize_dtype_memory(df: pd.DataFrame):
    for col in df.columns:
        if df[col].dtype.kind in "bifc":
            numeric_data = df[col].dropna()
            if numeric_data.empty:
                continue

            min_val = numeric_data.min()
            max_val = numeric_data.max()

            if np.isfinite(min_val) and np.isfinite(max_val):
                if np.issubdtype(numeric_data.dtype, np.integer):
                    if (
                        min_val >= np.iinfo(np.int8).min
                        and max_val <= np.iinfo(np.int8).max
                    ):
                        df[col] = df[col].astype(np.int8)
                    elif (
                        min_val >= np.iinfo(np.int16).min
                        and max_val <= np.iinfo(np.int16).max
                    ):
                        df[col] = df[col].astype(np.int16)
                    elif (
                        min_val >= np.iinfo(np.int32).min
                        and max_val <= np.iinfo(np.int32).max
                    ):
                        df[col] = df[col].astype(np.int32)
                    else:
                        df[col] = df[col].astype(np.int64)
                else:
                    if (
                        min_val >= np.finfo(np.float16).min
                        and max_val <= np.finfo(np.float16).max
                    ):
                        df[col] = df[col].astype(np.float16)
                    elif (
                        min_val >= np.finfo(np.float32).min
                        and max_val <= np.finfo(np.float32).max
                    ):
                        df[col] = df[col].astype(np.float32)
                    else:
                        df[col] = df[col].astype(np.float64)
    return df


def import_class(path: str) -> type:
    *module_path, class_name = path.split(".")
    module_path = ".".join(module_path)
    module = importlib.import_module(module_path)
    return getattr(module, class_name)


def _sanitize_dates(start: Union[int, date, datetime], end: Union[int, date, datetime]) -> SanitizeType:
    """
        Return (datetime_start, datetime_end) tuple
    """
    if start and end:
        if start >= end:
            raise ValueError("end must be after start")
    else:
        raise ValueError("start and or end must contain valid str, int, date or datetime object")

    start = datetime(start, 1, 1) if _types.is_number(start) else pd.to_datetime(start)
    end = datetime(end, 1, 1) if _types.is_number(end) else pd.to_datetime(end)

    return start, end


def _format_date(dt: Optional[datetime]) -> Optional[str]:
    """
        Returns formatted date
    """
    return None if dt is None else dt.strftime("%Y-%m-%d")


def _sanitize_time(start=None, end=None, interval=None, default_gap=DEFAULT_INTERVAL):
    if start and end:
        start, end = _sanitize_dates(start, end)
    if end is None:
        end = pd.to_datetime(datetime.today().date()) + timedelta(hours=16)
    if start is None: 
        if interval == "1m":
            start = end - timedelta(minutes=(default_gap - 1))
        elif interval == "5m":
            start = end - timedelta(minutes=((default_gap * 5) - 1))
        elif interval == "1h":
            start = end - timedelta(hours=(default_gap - 1))
        else:
            start = end - timedelta(days=(default_gap - 1))
    return str(_datetime_to_timestamp(start)), str(_datetime_to_timestamp(end))

def _format_datetime(dt: Optional[datetime]) -> Optional[str]:
    """
        Returns formatted date
    """
    return None if dt is None else dt.strftime("%Y-%m-%d %H:%m:%s")

def _datetime_to_timestamp(dt):
    return None if dt is None else datetime.timestamp(dt)
    

def previous_day_last_second():
    """Returns the last second of the previous day"""

    yesterday = datetime.today() - timedelta(days=1)
    return str(yesterday.date()) + " 23:59:59"


def previous_day_last_minute():
    """Returns the last minute of the previous day"""

    yesterday = datetime.today() - timedelta(days=1)
    return str(yesterday.date()) + " 23:59:00"

def get_today_latest_time() -> datetime: 
    return pd.to_datetime(datetime.today().date()) + timedelta(hours=23, minutes=59, seconds=59)

def index_df(df, index="timestamp", inplace=True, drop=False, time_field="timestamp"):
    if time_field:
        df[time_field] = pd.to_datetime(df[time_field])

    if inplace:
        df.set_index(index, drop=drop, inplace=inplace)
    else:
        df = df.set_index(index, drop=drop, inplace=inplace)

    if type(index) == str:
        df = df.sort_index()
    elif type(index) == list:
        df.index.names = index
        level = list(range(len(index)))
        df = df.sort_index(level=level)
    return df


def normal_index_df(df, category_field="code", time_filed="timestamp", drop=True, default_entity="entity"):
    if type(df) == pd.Series:
        df = df.to_frame(name="value")

    index = [category_field, time_filed]
    if is_normal_df(df):
        return df

    if df.index.nlevels == 1:
        if (time_filed != df.index.name) and (time_filed not in df.columns):
            assert False
        if category_field not in df.columns:
            df[category_field] = default_entity
        if time_filed not in df.columns:
            df = df.reset_index()

    return index_df(df=df, index=index, drop=drop, time_field="timestamp")

def pd_is_not_null(df: Union[pd.DataFrame, pd.Series]):
    return df is not None and not df.empty

def is_normal_df(df, category_field="code", time_filed="timestamp"):
    if pd_is_not_null(df) and df.index.nlevels == 2:
        names = df.index.names

        if len(names) == 2 and names[0] == category_field and names[1] == time_filed:
            return True

    return False

