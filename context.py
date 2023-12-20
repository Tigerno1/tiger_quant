import os 
import pandas as pd 
import tqdm 
import datetime
from typing import Union
from sqlalchemy import create_engine 
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.ext.declarative import DeclarativeMeta
from functools import partial 

from logger import LOG
from conf import DATA_PATH
from check import ReturnType, ProviderType
from utils import _sanitize_dates



from threading import Lock
class Singleton(type):
    _instances = {}
    _lock: Lock = Lock()
    def __call__(cls, *args, **kwargs):
        with cls._lock: 
            if cls not in cls._instances:
                instance = super(Singleton, cls).__call__(*args, **kwargs)
                cls._instances[cls] = instance
            return cls._instances[cls]

class Context(Singleton):
    _engine_map = dict()
    _session_map = dict()
    _schema_map = dict()
 
    def _get_engine(self, provider=None, db_full_name=None):
        assert provider is not None or db_full_name is not None 
        if not provider:
            provider = db_full_name.split("_")[0]
        if self._engine_map.get(provider):
            return self._engine_map[provider]
        db_path = os.path.join(DATA_PATH, "{}.db?check_same_thread=False".format(provider))
        db_engine = create_engine("sqlite:///" + db_path, echo=False)
        self._engine_map[provider] = db_engine
        return db_engine
    
    def _get_session(self, tb_full_name, force_new=False):
        if not force_new and self._session_map.get(tb_full_name):
            return self._session_map[tb_full_name]
        Session = scoped_session(sessionmaker())
        session = Session()
        self._session_map[tb_full_name] = session 
        return session 
    
    def _get_schema(self, tb_full_name):
        if self._schema_map.get(tb_full_name):
            return self._schema_map[tb_full_name]
        
    def _schema_get_session(self, schema, force_new=False):
        tb_full_name = schema.__tablename__
        session = self._get_session(tb_full_name, force_new)
        return session 
    
    def register_schema(
        self, 
        provider: ProviderType, 
        base: DeclarativeMeta
    ):
        engine = self._get_engine(provider)
        for item in base.registry.mappers:
            cls = item.class_
            tb_full_name = cls.__tablename__
            self._schema_map[tb_full_name] = cls 
            provider = tb_full_name.split("_")[0]
            session = self._get_session(tb_full_name)
            session.configure(bind=engine)
        base.metadata.create_all(engine)
    
    def unregister_schema(
        self, 
        schema_list: list, 
        base: DeclarativeMeta      
    ):
        tb_list = []
        for schema in schema_list: 
            tb_full_name = schema.__name__
            engine = self._get_engine(tb_full_name)
            tb_list.append(schema.__table__)
            if session:=self._session_map.get(tb_full_name):
                session.close()
                del self._session_map[tb_full_name] 
            if self._schema_map.get(tb_full_name):
                del self._schema_map[tb_full_name]
        base.metadata.drop_all(engine, tables=tb_list)

    def get_data(
        self,
        schema,
        code: list|str = None, 
        start: Union[int, datetime.date, datetime.datetime, str] = None, 
        end: Union[int, datetime.date, datetime.datetime, str] = "2050-01-01", 
        on: Union[int, datetime.date, datetime.datetime, str] = None, 
        exchanges: list = None,
        columns: list = None, 
        order = None, 
        limit: int = None, 
        filters: list = None, 
        return_type: ReturnType= "df",
        index: list|str = None,
        time_field: str = "timestamp"
        ):
        assert schema is not None 
        session = self._schema_get_session(schema)
        if columns:
        # support str
            for i, col in enumerate(columns):
                if isinstance(col, str):
                    columns[i] = eval("schema.{}".format(col))

            query = session.query(*columns)
        else:
            query = session.query(schema)

        if code: 
            if isinstance(code, str):
                query = query.filter(schema.code == code)
            if isinstance(code, list):
                query = query.filter(schema.code.in_(code))

        time_col = eval("schema.{}".format(time_field))

        if start: 
            start_date, end_date = _sanitize_dates(start, end)
            query = query.filter(time_col >= start_date)
            query = query.filter(time_col <= end_date)
            
        elif on: 
            date = _sanitize_dates(date, date)[0]
            query = query.filter(time_col == date)
        
        if exchanges:
            query = query.filter(schema.exchange.in_(exchanges))
        if filters:
            for filter in filters:
                query = query.filter(filter)
        if order is not None:
            query = query.order_by(order)
        else:
            query = query.order_by(time_col.asc())
        if limit:
            query = query.limit(limit)

        if return_type == "df":
            df = pd.read_sql(query.statement, query.session.bind, index_col=index)
            return df
        elif return_type == "domain":
            return query.all()
        elif return_type == "dict":
            return [item.__dict__ for item in query.all()]

    def del_data(self, schema, filters: list = None):
        """
        delete data by filters

        :param data_schema: data schema
        :param filters: filters
        :param provider: data provider
        """

        session = self._schema_get_session(schema)
        query = session.query(schema)
        if filters:
            for f in filters:
                query = query.filter(f)
        query.delete()
        session.commit()
        


    def query(self, provider, sec_type, tb_name, *args, **kwargs):
        table_full_name = "_".join([provider, sec_type, tb_name])
        schema = self._schema_map[table_full_name]
        schema.query(*args, **kwargs)

    def _get_ids(self, schema, ids):
        tb_full_name = schema.__tablename__
        session = self._get_session(tb_full_name)
        id_data = session.query(schema.id).filter(schema.id.in_(ids))
        return [id[0] for id in id_data]


    def _iter_df(self, df, sub_size):
        size = len(df)
        if size >= sub_size: 
            step_size = int(size / sub_size)
            if size % sub_size:
                step_size = step_size + 1
        else:
            step_size = 1 
        for step in tqdm(range(step_size)):
            yield df.iloc[sub_size*step: sub_size * (step+ 1)]

    
    def save(
        self,
        df: pd.DataFrame,
        schema: DeclarativeMeta,
        force_update: bool = False,
        sub_size: int = 5000,
        drop_duplicates: bool = False
    ) -> object:
        tb_full_name = schema.__tablename__
        if df is None or df.empty:
            return 
        if drop_duplicates and df.duplicated(subset='id').any():
            LOG.warning(f'remove duplicated:{df[df.duplicated()]}')
        df = df.drop_duplicates(subset='id', keep='last')
        columns = schema.__table__.columns.keys()
        cols = set(df.columns.tolist()) & set(columns)
        if not cols:
            print("wrong cols")
            return 
        df = df[list(cols)]
        session = self._get_session(tb_full_name)
        engine = self._get_engine(tb_full_name)
        iter_df = partial(self._iter_df, sub_size)
        for sub_df in iter_df(df):
            ids = sub_df["id"].tolist()
            if force_update:
                if len(ids) == 1:
                    sql = f'delete from `{tb_full_name}` where id = "{ids[0]}"'
                else:
                    sql = f"delete from `{tb_full_name}` where id in {tuple(ids)}"

                session.execute(sql)
                session.commit()
            else: 
                db_ids = self._get_ids(schema, ids)
                if db_ids:
                    sub_df = sub_df[~sub_df["id"].isin(db_ids)]
                session.commit() 

            if sub_df is not None and not sub_df.empty:
                sub_df.to_sql(tb_full_name, engine, index=False, if_exists="append")




        

         
tg_context = Context()