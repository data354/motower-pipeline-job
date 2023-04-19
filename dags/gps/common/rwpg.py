import pandas as pd
import psycopg2


def write_pg(host: str, database:str, user: str, password: str, data, table: str = None):
    """"
     write data in pg
    """
    with psycopg2.connect(host= host,database= database,user= user,password= password) as conn:
            data.to_sql(table, conn, if_exists="append")