import pandas as pd 

def index_df(df: pd.DataFrame, index="timestamp", inplace=True, drop=False, time_field="timestamp"):
    if time_field:
        df[time_field] = pd.to_datetime(df[time_field])

    if inplace:
        df.set_index(index, drop=drop, inplace=inplace)
    else:
        df = df.set_index(index, drop=drop, inplace=inplace)

    if isinstance(index, str):
        df = df.sort_index()
    elif isinstance(index, list):
        df.index.names = index
        level = list(range(len(index)))
        df = df.sort_index(level=level)
        
    return df