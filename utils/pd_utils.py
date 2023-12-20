import pandas as pd  
from typing import Union

def pd_is_not_null(df: Union[pd.DataFrame, pd.Series]):
    return df is not None and not df.empty