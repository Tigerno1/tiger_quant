from typing import Optional, Callable
import pandas as pd 
import functools
import traceback
from requests.exceptions import RetryError, ConnectTimeout
from logger import LOG

Handle_Request_Type = Callable[..., Optional[pd.DataFrame]]
def _handle_request_errors(func: Handle_Request_Type) -> Optional[Handle_Request_Type]:
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except ConnectionError:
            LOG.debug(msg = "Connection Error", exc_info= traceback.format_exc())
            return None
        except RetryError:
            LOG.debug(msg = "Retry Error", exc_info= traceback.format_exc())
            return None
        except ConnectTimeout:
            LOG.debug(msg = "Connection Error", exc_info= traceback.format_exc())
            return None

    return wrapper


def _handle_environ_error(func: Handle_Request_Type) -> Optional[Handle_Request_Type]:
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            api_key: Optional[str] = kwargs.get('api_key')
            assert api_key is not None
            assert api_key != ""
            return func(*args, **kwargs)
        except AssertionError:
            raise EnvironNotSet("Environment not set, see readme.md on how to setup your environment variables")

    return wrapper


# Errors

class RemoteDataError(IOError):
    """
    Remote data exception
    """
    pass


class EnvironNotSet(Exception):
    """
        raised when environment variables are not set
    """
    pass
