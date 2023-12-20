import os 
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker 
from sqlalchemy.orm import DeclarativeMeta

from os import path as ospath 
from sys import path as syspath 
# path = ospath.dirname(ospath.abspath(__file__))

# syspath.append(path)
# print
from tiger_quant.conf import DATA_PATH

__engine_map = {}
__session_map = {} 

def __get_engine(provider):
    if __engine_map.get:
        return __engine_map[provider]
    db_path = os.path.join(DATA_PATH, "{}.db?check_same_thread=False".format(provider))
    db_engine = create_engine("sqlite:///" + db_path, echo=False)
    __engine_map[provider] = db_engine
    return db_engine

def __get_session(tb_full_name, force_new=False):
    if not force_new and __session_map.get(tb_full_name):
        return __session_map[tb_full_name]
    session = sessionmaker()
    __session_map[tb_full_name] = session
    return session 

def register(provider, base, schema_list=None):
    engine = __get_engine(provider)
    if schema_list: 
        base.metadata.create_all(schema_list, checkfirst=True)
    else: 
        base.metadata.create_all(engine, checkfirst=True)
    for item in base.register.mappers:
        cls = item.class_ 
        if type(cls) == DeclarativeMeta: 
            schema_name = cls.__tablename__ 
            session = __get_session(schema_name)
            session.configure(bind=engine)
    return 

class Schema:
    __tablename__ = None 
    __session = __get_session(__tablename__) 
       
    def query_data(cls, ):
        pass
    def df_to_db(cls, ):
        pass 




