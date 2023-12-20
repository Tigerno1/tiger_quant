import os 
import time
import asyncio 
import aiohttp
import requests 
import traceback 
import pandas as pd 
from io import StringIO 
from functools import wraps
from urllib.parse import urlparse 
from typing import Tuple, Dict

from logger import LOG
from conf import CONN_TIMEOUT, READ_TIMEOUT, PROXY


def retry(max_retries=3, wait_interval=5):
    def decor(fun):
        @wraps(fun)
        def inner(*args, **kwargs):
            n = 0
            while n < max_retries: 
                try:
                    return fun(*args, **kwargs)
                except Exception:
                    LOG.warn(
                        f"接口报错，{wait_interval}秒后尝试第{n + 1}/{max_retries}次. \n\n {traceback.format_exc()}"
                    )
                    asyncio.sleep(wait_interval)
                    n += 1
        return inner 
    return decor 

def async_retry(max_retries=3, wait_interval=5):
    def decor(fun):
        @wraps(fun)
        async def inner(*args, **kwargs):
            n = 0
            while n < max_retries: 
                try:
                    return await fun(*args, **kwargs)
                except Exception:
                    LOG.warn(
                        f"接口报错，{wait_interval}秒后尝试第{n + 1}/{max_retries}次. \n\n {traceback.format_exc()}"
                    )
                    time.sleep(wait_interval)
                    n += 1
        return inner 
    return decor 

class Requester:   
    format = format
    session = requests.Session()

    def _create_params(self, *args, **kwargs) -> Tuple[str, Dict[str, str], Dict[str, str]]:
        """
            **create_params**
                will create parameters to pass into request session
        """
        raise NotImplementedError
    
    async def _async_create_params(self, *args, **kwargs) -> Tuple[str, Dict[str, str], Dict[str, str]]:
        """
            **create_params**
                will create parameters to pass into request session
        """
        raise NotImplementedError
        
    @retry()
    def _get(self, url, params=None, data=None,
                proxy=PROXY, conn_timeout=CONN_TIMEOUT, 
                read_timeout=READ_TIMEOUT):
        resp = self.session.get(
                url, 
                proxies= proxy, 
                params=params,
                timeout=(conn_timeout, read_timeout) 
                )
        data = self.format.form(resp)
        return data 
        

    @retry()
    def _post(self, url, params=None, data=None, 
                proxy=PROXY, conn_timeout=CONN_TIMEOUT, 
                read_timeout=READ_TIMEOUT):
        resp = self.session.post(
                url, 
                proxies= proxy, 
                params=params,
                timeout=(conn_timeout, read_timeout) 
                )
        data = self.format.form(resp)
        return data 

    @async_retry()
    async def _async_get(self, url, params=None, data=None, 
                proxy=PROXY, conn_timeout=CONN_TIMEOUT, 
                read_timeout=READ_TIMEOUT):
        async with aiohttp.ClientSession() as session:
            timeout = aiohttp.ClientTimeout(total=conn_timeout, sock_read=read_timeout)
            async with session.get(
                urlparse(url).geturl(), 
                params=params, 
                data=data,
                timeout=timeout, 
                proxy=proxy) as resp:
                return await self.format.form(resp)
            
    @async_retry()
    async def _async_post(self, url, params=None, data=None, 
                proxy=PROXY, conn_timeout=CONN_TIMEOUT, 
                read_timeout=READ_TIMEOUT):
        async with aiohttp.ClientSession() as session:
            timeout = aiohttp.ClientTimeout(total=conn_timeout, sock_read=read_timeout)
            async with session.post(
                urlparse(url).geturl(), 
                params=params, 
                data = data,
                timeout=timeout, 
                proxy=proxy) as resp:
                return await self.format.form(resp)
    
                
class JsonFormat:
    def form(resp):
        return resp.json()

class AsyncJsonFormat:
    async def form(resp):
        return await resp.json()

class TextFormat:
    def form(resp):
        data = resp.text
        return pd.read_csv(StringIO(data), engine='python', skipfooter=0, parse_dates=[0], index_col=0)

class AsyncTextFormat:
    async def form(resp):
        data = await resp.text()
        # Here we're not using await with pd.read_csv as it's not an async function
        return pd.read_csv(StringIO(data), engine='python', skipfooter=0, parse_dates=[0], index_col=0)

        