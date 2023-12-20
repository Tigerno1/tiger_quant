from typing import Union 
from sqlalchemy.ext.declarative import DeclarativeMeta
from sqlalchemy.schema import Index
from logger import init_logger

log = init_logger(__name__)
from api.eod import get_engine, get_session

def register_schema(provider, 
                    base: DeclarativeMeta,
                    ):
    schema_name = schema.__tablename__
    provider = schema_name.spllit("_")
    engine = get_engine(provider)
    try:
        base.metadata.create_all(engine, checkfirst=True)
        session = get_session(schema_name)
        session.configure(bind=engine)

        # Get existing indices
        with engine.connect() as con:
            rs = con.execute("PRAGMA INDEX_LIST('?')", (schema_name,))
            existing_indices = (row[1] for row in rs)
            
        # logger.debug("engine:{},table:{},index:{}".format(engine, schema_name, index_list))

        # Create indices if they don't exist
        for col in index:
            columns_to_index = col if isinstance(col, (list, tuple)) else [col]
            index_name = "{}_{}_index".format(schema_name, "_".join(columns_to_index))
            if index_name not in existing_indices and all(getattr(schema, c, None) for c in columns_to_index):
                Index(index_name, *columns_to_index).create(engine)
    except Exception as e:
        log.error(f"Error while registering schema: {e}")

if __name__ == "__main__":
    register_schema("my_us_stock_1d_kdata")

