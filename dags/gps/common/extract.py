""" EXTRACTION COMMONS"""
import logging
from io import BytesIO
import ftplib
from functools import lru_cache
from typing import Tuple, Union, Any
from datetime import datetime
from unidecode import unidecode
import pandas as pd
import psycopg2
from psycopg2.extensions import connection
from gps import CONFIG 

# Define SQL queries for different tables   ks_tdb_radio_drsi
SQL_QUERIES = { 
    "hourly_datas_radio_prod": """select  date_jour, code_site, sum(trafic_voix) as trafic_voix, 
    sum(trafic_data) as trafic_data, techno from 
    hourly_datas_radio_prod where date_jour = %s group
    by date_jour, code_site, techno;""",

    "hourly_datas_radio_prod_archive": """select  date_jour, code_site,
    sum(trafic_voix) as trafic_voix,
    sum(trafic_data) as trafic_data, techno from 
    hourly_datas_radio_prod_archive where date_jour = %s group by date_jour, code_site, techno;""",

    "ks_tdb_radio_drsi": ''' select * from "ENERGIE"."KS_TDB_RADIO_DRSI" where "DATE_ID" = %s ;''' ,   
    
    "ks_hebdo_tdb_radio_drsi": ''' select * from "ENERGIE"."KS_HEBDO_TDB_RADIO_DRSI" where EXTRACT(WEEK FROM TO_DATE("DATE_ID", 'YYYY-MM-DD')) = %s AND EXTRACT(YEAR FROM TO_DATE("DATE_ID", 'YYYY-MM-DD') ) = %s ''',

    "ks_daily_tdb_radio_drsi": ''' select * from "ENERGIE"."KS_DAILY_TDB_RADIO_DRSI" where "DATE_ID" = %s ''',

    "Taux_succes_2g": """select date_jour, SPLIT_PART(bcf_name,'_',1) AS code_site,
    MIN(CAST(cssr_cs AS DECIMAL)) AS min_cssr_cs,
    MAX(CAST(cssr_cs AS DECIMAL)) AS max_cssr_cs, AVG(CAST(cssr_cs AS DECIMAL)) AS avg_cssr_cs,
    median(CAST(cssr_cs AS DECIMAL)) AS median_cssr_cs, techno
    from Taux_succes_2g where date_jour=%s group by date_jour, code_site, techno;""",

    "Call_drop_2g": """select date_jour, SPLIT_PART(bcf_name,'_',1) AS code_site,
    SUM(CAST(drop_after_tch_assign AS INTEGER)) as drop_after_tch_assign, techno 
    from Call_drop_2g where date_jour=%s group by date_jour, code_site, techno;""",

    "Call_drop_3g": """select date_jour, SPLIT_PART(wbts_name,'_',1) AS code_site, 
    SUM(CAST(number_of_call_drop_3g AS INTEGER)) as number_of_call_drop_3g, techno 
    from Call_drop_3g where date_jour=%s group by date_jour, code_site, techno;""",

    "Taux_succes_3g": '''select date_jour, SPLIT_PART(wbts_name,'_',1) AS code_site,
    MIN(CAST("3g_call_setup_suceess_rate_speech_h" AS DECIMAL)) as min_cssr_cs,
    MAX(CAST("3g_call_setup_suceess_rate_speech_h" AS DECIMAL)) as max_cssr_cs,
    AVG(CAST("3g_call_setup_suceess_rate_speech_h" AS DECIMAL)) as avg_cssr_cs,
    median(CAST("3g_call_setup_suceess_rate_speech_h" AS DECIMAL)) as median_cssr_cs,
    techno from Taux_succes_3g where date_jour=%s
    group by date_jour, code_site, techno;''',

    "faitalarme": """select  *, nbrecellule * delay as delayCellule
    from faitalarme where date=%s;""",
    
    
    "dtm_motower_gsm": '''select date_id as day_id, id_site, ca_voix , ca_data, ca_total, 
    trafic_voix as trafic_voix_in, trafic_data as trafic_data_in ,parc_mois as parc, parc_data_otarie as parc_data, parc_2g, 
    parc_3g, parc_4g, parc_5g, parc_other from "DDIR".dtm_motower_gsm where date_id=%s;
    '''  
}


@lru_cache(maxsize=None)
def get_connection(host: str, database: str, user: str, password: str):
    """
    Function to get a connection to the PostgreSQL server
    Args:
        host [str]: host name
        database [str]: database name
        user [str]: user name
        password [str]: password
    Return:
        psycopg2 connection object
    """
    return psycopg2.connect(host=host, database=database, user=user, password=password)




def execute_query(args: Tuple[Union[connection, str, Any], ...]) -> pd.DataFrame:
    """
    Function to execute a SQL query
    Args:
        args [tuple]: (table, date, sql_query) or (table, week, year, sql_query)
    Return:
        pandas DataFrame
    """
    if len(args) not in [3, 4]:
        raise ValueError("Invalid number of arguments")

    conn, sql_query = args[0], args[-1]
    params = args[1:-1]

    if len(args) == 4:
        week, year = params
        logging.info("get data to date %s and year %s", week,year)
        df_ = pd.read_sql_query(sql_query, conn, params=params)
    else:
        date = params[0]
        logging.info("get data to date %s", date)
        df_ = pd.read_sql_query(sql_query, conn, params=params)

    return df_


def extract_pg(host: str, database: str, user: str, password: str, table: str = None,
               date: str = None, sql_query: str = None) -> pd.DataFrame:
    """
    Function to get data from table and save in parquet file
    Args:
        table [str]: table name
        date [str]
    Return:
        pandas DataFrame
    """
    # get connection
    try:
        conn = get_connection(host, database, user, password)
    except Exception as e:
        logging.error("Could not connect to database: %s", e)
        return None
    
    # Log the table and date information
    logging.info("Getting data of the table %s ", table)
    logging.info("where date is %s", date)
    if sql_query is not None:
        return execute_query((conn, date, sql_query))
    
    if table is None:
        raise ValueError("Table name is required")
    
    if table not in SQL_QUERIES:
        raise ValueError("Invalid table name")
    
    query_params: Tuple = ()
    
    if table == "ks_tdb_radio_drsi":
        query_params = (date[:-2]+"01", SQL_QUERIES[table])
    elif table in ["ks_daily_tdb_radio_drsi"]:
        query_params = (date, SQL_QUERIES[table])
    elif table in ["ks_hebdo_tdb_radio_drsi" ]:
        exec_date = datetime.strptime(date, CONFIG["date_format"])
        exec_week, exec_year = exec_date.isocalendar()[1], exec_date.isocalendar()[0]
        query_params = (exec_week, exec_year, SQL_QUERIES[table])
    elif table != "faitalarme":
        query_params = (date.replace("-",""), SQL_QUERIES[table])
    else:
        query_params = (date, SQL_QUERIES[table])
    query_params = (conn, ) + query_params
    return execute_query(query_params)




def list_ftp_file(hostname: str, user: str, password: str)-> list:
    """
        list files on FTP server
    """
    try:
        server = ftplib.FTP(hostname, user, password, timeout=200)
    except Exception as error :
        raise ConnectionError("Connection to FTP server failed.") from error
    logging.info("check file")
    server.cwd(CONFIG["ftp_dir"])
    file_list = server.nlst()
    server.quit()  # Close the FTP connection
    return file_list

def extract_ftp(hostname: str, user: str, password: str, date: str) -> pd.DataFrame:
    """
    Connect to ftp server and get file
    ARGS:
        hostname[str]: ftp server name
        user[str]: ftp username
        password[str]: ftp password
        date[str]
    RETURN:
        pd.DataFrame
    """
 

    # Connect to the FTP server
    
    try:
        server = ftplib.FTP(hostname, user, password, timeout=300)
        server.cwd(CONFIG["ftp_dir"])
    except ftplib.error_perm as err:
        raise OSError( 
            f"{CONFIG['ftp_dir']} don\'t exist on FTP server") from err
    except Exception as error:
        raise ConnectionError("Connection to FTP server failed.") from error
    # Download the file from the FTP server
    filename = f'extract_vbm_{date.replace("-", "")}.csv'
    logging.info("Get %s", filename)
    downloaded = BytesIO()
    try:
        server.retrbinary(f'RETR {filename}', downloaded.write)
        logging.info("Read data")
        #Read the downloaded file into a DataFrame
        downloaded.seek(0)
        df_ = pd.read_csv(downloaded, engine="python", sep=";")
    except Exception as error:
        raise ValueError(
            "Failed to read the downloaded file into a DataFrame.") from error
    finally:
        server.quit()
    # Verify if the file is empty
    if df_.empty:
        raise RuntimeError(f"{filename} is empty")
    # Verify if the required columns are present
    df_.columns = df_.columns.str.lower().map(unidecode)
    good_columns = [d["columns"]
                    for d in CONFIG["tables"] if d["name"] == "caparc"][0]
    missing_columns = set(good_columns).difference(set(df_.columns))
    if missing_columns:
        raise ValueError(f"missing columns {', '.join(missing_columns)}")
    # Add "month_id" columns
    new_trafic_names = {'trafic_data': 'trafic_data_in',
                    'trafic_voix': 'trafic_voix_in'}
    df_.rename(columns=new_trafic_names, inplace=True)
    try:
        logging.info("add column")
        df_["day_id"] = df_["day_id"].astype(str)
        df_["month_id"] = df_["day_id"].str[:4].str.cat(
            df_["day_id"].str[4:6], "-")
    except Exception as error:
        raise ValueError("Failed to add the 'month_id' column.") from error
    return df_
