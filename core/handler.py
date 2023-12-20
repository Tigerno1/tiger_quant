from __future__ import annotations
from abc import ABC, abstractmethod
from typing import Any, Optional, List, Union

import pandas as pd 
from decimal import Decimal
from utils.pd_utils import pd_is_not_null


class Handler(ABC):
    """
    The Handler interface declares a method for building the chain of handlers.
    It also declares a method for executing a request.
    """

    @abstractmethod
    def set_next(self, handler: Handler) -> Handler:
        pass

    @abstractmethod
    def handle(self, data: Any, *args, **kwargs) -> Optional[str]:
        pass


class AbstractHandler(Handler):
    """
    The default chaining behavior can be implemented inside a base handler
    class.
    """

    _next_handler: Handler = None
    @property 
    def parent(self) -> AbstractHandler:
        return self._parent
    @parent.setter
    def parent(self, parent: AbstractHandler):
        self._parent = parent 

    def set_next(self, handler: Handler) -> Handler:
        self._next_handler = handler
        return handler

    @abstractmethod
    def handle(self, data):
        if self._next_handler:
            return self._next_handler.handle(data)
        return data
    
class DictSpliter(AbstractHandler):
    def __init__(self, field: Union[List, str, None]=None):
        self.field = field 
    def handle(self, data):
        if data is None:
            return 
        field_list = []
        if isinstance(self.field, str): 
            return data[self.field]
        if isinstance(self.field,list): 
            for field in self.field:
                field_list.append(data[field])
            return super().handle(field_list)
        return super().handle(data)
    
class DfFormatter(AbstractHandler): 
    def handle(self, data):
        if data is None:
            return 
        if not isinstance(data, pd.DataFrame):
            return super().handle(pd.DataFrame(data))
        return super().handle(data)

class DictFormatter(AbstractHandler):
    def __init__(self, date_field: Union[str, int, float, List,  None], region):
        self.date_field = date_field
        self.region = region 
    def handle(self, data):
        if data is None:
            return 
        if isinstance(self.date_field, list):
            for date in self.date_field: 
                date = self.handle_single(data, date)
                data[date] = date
            return super().handle(data)
        if isinstance(self.date_field, str):
            return super().handle(self.handle_single(data, date=self.date_field))

    def handle_single(self, data, date):
        if date is None or data.get(date) is None:
            return data
        if isinstance(data.get(date), str):
            data[date] = pd.Timestamp(data[date])
        if isinstance(data.get(date), int):
            data[date] = pd.Timestamp.fromtimestamp(data[date]/ 1000)
        if isinstance(data.get(date), float):
            data[date] = pd.Timestamp.fromtimestamp(data[date])
        return data

class DfFormatter(AbstractHandler):
    def __init__(self, date_field: Union[str, int, float, List,  None], region):
        self.date_field = date_field
        self.region = region 
    def handle(self, data): 
        if data is None or data.empty:
            return 
        if isinstance(self.date_field, list):
            for date in self.date_field: 
                data = self.handle_single(data, date=date)
            return super().handle(data)
        if isinstance(self.date_field, str):
            return super().handle(self.handle_single(data), date=self.date_field)
    
    def handle_single(self, data, date):
        if date is None or date not in data.columns:
            return super().handle(data)
        if isinstance(data[date], str):
            data[date].apply(pd.Timestamp(data[date]), axis=1, inplace=True)
        if isinstance(data[date], int):
            data[date].apply(pd.Timestamp.fromtimestamp(data[date]/ 1000), axis=1, inplace=True)
        if isinstance(data[date], float):
            data[date].apply(pd.Timestamp.fromtimestamp(data[date]), axis=1, inplace=True)
        return super().handle(data)
    
class DictRenamer(AbstractHandler):
    def __init__(self, rename_map):
        self.rename_map = rename_map
    def handle(self, data):
        for k, v in self.rename_map.items():
            data[v] = data.pop([k])
        return super().handle(data)
        
class DfRenamer(AbstractHandler):
    def __init__(self, rename_map):
        self.rename_map = rename_map
    def handle(self, data):
        return super().handle(data.rename(columns=self.rename_map, inplace=True))

class DictAdder(AbstractHandler):
    def __init__(self, add_map):
        self.add_map = add_map
    def handle(self, data):
        for k, v in self.add_map.items(): 
            data[k] = v
        return super().handle(data)

class DfAdder(AbstractHandler):
    def __init__(self, add_map):
        self.add_map = add_map
    def handle(self, data): 
        keys = self.add_map.keys()
        values = self.add_map.values()
        data[keys] = values
        return super().handle(data) 
        
class DictFormatModifier(AbstractHandler):
    def __init__(self, format_map):
        self.format_map = format_map
    def handle(self, data):
        for k, v in self.format_map.items():
            data[k] = v(data[k])
        return super().handle(data)

class DfFormatModifier(AbstractHandler):
    def __init__(self, format_map):
        self.format_map = format_map
    def handle(self, data):
        for k, v in self.format_map.items():
            data[k] = data[k].apply(v, axis=1)
        return super().handle(data)
    
class DictDropDuplicated(AbstractHandler):
    def __init__(self, fields):
        self.fields = fields
    def handle(self, data):
        for field in self.fields:
            dt = data.get(field)
            if dt is None:
                continue

class DfDropDuplicated(AbstractHandler):
    def __init__(self, fields):
        self.fields = fields
    def handle(self, data):
        if data.duplicated(subset=self.fields).any():
            data.drop_duplicates(subset='id', keep='last', inplace=True)
            return super().handle(data)
        return super().handle(data)

class DictFloatHFormatter(AbstractHandler):
    def __init__(self, fields):
        self.fields = fields
    def handle(self, data):
        for field in self.fields:
            if data.get(field) and isinstance(data[field], str):
                target_data = data[field]
                if "%" in target_data:
                    data[field] = float(Decimal(target_data.replace("%", "")/Decimal(100)))
                if "," in data[field]:
                    data[field] = float(Decimal(data[field].replace(",", "")))
        return super().handle(data)

class DictLowCaseHandler(AbstractHandler):
    def handle(self, data):
        for k in data:
            if isinstance(k, str) and not k.islower():
                data[k.lower()] = data.pop(k)
        return super().handle(data)
    
class DictDeleteHandler(AbstractHandler):
    def __init__(self, fields):
        self.fields = fields
    def handle(self, data):
        for field in self.fields:
            if data.get(field):
                data.pop(field)
        return super().handle(data)

class PersistHandler(AbstractHandler):
    def __init__(self, domain):
        self.domain = domain

    def handle(self, data):
        for k, v in data.items(): 
            if getattr(self.domain, k):
                setattr(self.domain, k, v)
        return self.domain

    
class CompositeHandler(AbstractHandler):
    def __init__(self):
        self._children: List[AbstractHandler] = []
    
    @property 
    def parent(self) -> AbstractHandler:
        return self._parent
    @parent.setter
    def parent(self, parent: AbstractHandler):
        self._parent = parent 

    def add_handler(self, handler):
        self._children.append(handler)
        handler.parent = self 

    def remove_handler(self, handler):
        self._children.remove(handler)
        handler.parent = None 

    def handle(self, data):
        for dt in data:
            self._children.handle(dt)
        return data







        

        




