
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import MetaData
from sqlalchemy import Table
from sqlalchemy.orm import sessionmaker
from sqlalchemy.dialects.mysql import insert

def df_to_dict(df):
    rows = df.shape[0]
    lis = [dict(zip(list(df.columns), df.loc[i])) for i in range(rows)]
    return lis 


def upsert(table_cls, records, chunk_size=10000, commit_on_chunk=True, except_cols_on_update=[]):
    update_keys = [key for key in records[0].keys() if
                   key not in except_cols_on_update]
    if commit_on_chunk:
        for i in range(0, len(records), chunk_size):
            chunk = records[i:i + chunk_size]
            insert_stmt = insert(table_cls).values(chunk)
            update_columns = {x.name: x for x in insert_stmt.inserted if x.name in update_keys}
            upsert_stmt = insert_stmt.on_duplicate_key_update(**update_columns)
            with engine.connect() as conn:
                result = conn.execute(upsert_stmt)
    else:
        insert_stmt = insert(table_cls).values(records)
        update_columns = {x.name: x for x in insert_stmt.inserted if x.name in update_keys}
        upsert_stmt = insert_stmt.on_duplicate_key_update(**update_columns)
        with engine.connect() as conn:
            result = conn.execute(upsert_stmt)