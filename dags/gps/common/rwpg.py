import pandas as pd
from sqlalchemy import create_engine

def write_pg(host: str, database:str, user: str, password: str, data, table: str = None, port:str=5432):
    """"
     write data in pg
    """
    engine = create_engine(f'postgresql://{user}:{password}@:{host}:{port}/{database}')
    data.to_sql(table, engine, if_exists="append")