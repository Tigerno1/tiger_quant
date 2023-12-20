import os
from sqlalchemy import create_engine 
from sqlalchemy.orm import sessionmaker

from cacher import engine_cacher, session_cacher
from conf import DATA_PATH

def get_engine(provider):
    if engine_cacher.get(provider):
        return engine_cacher[provider]
    db_path = os.path.join(DATA_PATH, "{}.db?check_same_thread=False".format(provider))
    db_engine = create_engine("sqlite:///" + db_path, echo=False)
    engine_cacher[provider] = db_engine
    return db_engine
    

def get_session(provider, tb_full_name, force_new=False):
    engine = get_engine(provider)
    if not force_new and session_cacher.get(tb_full_name):
        return session_cacher[tb_full_name]
    Session = sessionmaker(bind=engine)
    session = Session()
    session_cacher[tb_full_name] = session 
    return session 



 



