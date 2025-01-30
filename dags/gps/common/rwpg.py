""" postgres UTILS"""
from datetime import datetime as dt
import psycopg2
from psycopg2   import sql
from sqlalchemy import create_engine
from gps import CONFIG


def create_table(cur, table_name: str, columns: dict):
    create_query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
        id SERIAL ,
        {", ".join([f"{column} {data_type}" for column, data_type in columns.items()])}
    
    """
    if table_name == "motower_daily_caparc":
        create_query += " , PRIMARY KEY(jour, code_oci_id) );"
    elif table_name in ("motower_daily_congestion", "motower_daily_trafic"):
        create_query += " , PRIMARY KEY(jour, id_site));"
    else:
        create_query += ", PRIMARY KEY(id) );"
    cur.execute(create_query)



def write_pg(host: str, database:str, user: str, password: str,
            data, table: str = None, port:str="5432"):
    """"
     write data in pg
    """
    # create table if not exists
    conn = psycopg2.connect(
        host=host,
        database=database,
        user=user,
        password=password,
        port = port
        )
    cur = conn.cursor()
    cur.execute(f"""SELECT EXISTS (SELECT FROM
        information_schema.tables WHERE table_name = '{table}');""")
    table_exists = cur.fetchone()[0]
    if not table_exists:
        columns = CONFIG["output_tables"][table]
        create_table(cur, table, columns)

    # delete data if already exists
    # Delete data from table
    if table in ["motower_daily", "motower_daily_caparc_faits", "motower_daily_congestion", "motower_daily_trafic","motower_daily_caparc_faits_new" ]:
        delete_query = sql.SQL("DELETE FROM {table} WHERE jour = %s;").format(table=sql.Identifier(table))
        cur.execute(delete_query, [str(data.jour.unique()[0])])
    elif table == "motower_weekly":
        exec_date = data["jour"].dt.to_pydatetime()[0]
        delete_query = sql.SQL("DELETE FROM {table} WHERE EXTRACT(WEEK FROM jour) = %s AND EXTRACT(YEAR FROM jour) = %s;").format(table=sql.Identifier(table))
        cur.execute(delete_query, [str(exec_date.isocalendar()[1]), str(exec_date.isocalendar()[0])])
    elif table in ["motower_monthly_dimensions", "motower_monthly_faits","motower_daily_caparc_dimension","motower_daily_caparc_dimension_new"]:
        delete_query = sql.SQL("DELETE FROM {table} WHERE mois = %s;").format(table= sql.Identifier(table))
        cur.execute(delete_query, [str(data.mois.unique()[0])])

    conn.commit()
    cur.close()

    # append data in table
    db_uri = f'postgresql://{user}:{password}@{host}:{port}/{database}'
    engine = create_engine(db_uri)
    data.to_sql(table, engine, index=False, if_exists = 'append')
    conn.close()







 
