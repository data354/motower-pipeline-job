""" EXTRACTION COMMONS"""
import logging
from io import BytesIO
import ftplib
from functools import lru_cache
from unidecode import unidecode
import pandas as pd
import psycopg2
from gps import CONFIG
from gps.common.alerting import send_email

# Define SQL queries for different tables

SQL_QUERIES = {
    "hourly_datas_radio_prod": """select  date_jour, code_site, sum(trafic_voix) as trafic_voix, 
    sum(trafic_data) as trafic_data, techno from 
    hourly_datas_radio_prod where date_jour = %s group
    by date_jour, code_site, techno;""",

    "hourly_datas_radio_prod_archive": """select  date_jour, code_site,
    sum(trafic_voix) as trafic_voix,
    sum(trafic_data) as trafic_data, techno from 
    hourly_datas_radio_prod_archive where date_jour = %s group by date_jour, code_site, techno;""",

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
    from faitalarme where date=%s;"""
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


def execute_query(args):
    """
    Function to execute a SQL query
    Args:
        args [tuple]: (table, date, sql_query)
    Return:
        pandas DataFrame
    """
    conn, date, sql_query = args
    
    df_ = pd.read_sql_query(sql_query, conn, params=(date,))
    conn.close()
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
    conn = get_connection(host, database, user, password)
    # Log the table and date information
    logging.info("Getting data of the table %s ", table)
    logging.info("where date is %s", date)
    if (table is None) and (date is None) and (sql_query is not None):
        return execute_query([conn, date, sql_query])
    if table in SQL_QUERIES:
        if table != "faitalarme":
            return execute_query([conn, date.replace("-",""), SQL_QUERIES[table]] )
        return execute_query([conn, date, SQL_QUERIES[table]] )
    raise RuntimeError("Please verify function params")

# def file_exists(hostname: str, user: str, password: str, date: str, smtp_host, smtp_user, smtp_port, receivers)-> bool:
#     """
#     """
#     filename = f'extract_vbm_{date.replace("-", "")}.csv'
#     logging.info("Get %s", filename)
#     downloaded = BytesIO()
#     try:
#         server = ftplib.FTP(hostname, user, password, timeout=200)
#         server.cwd(CONFIG["ftp_dir"])
#         logging.info("downloading....")
#         server.retrbinary(f'RETR {filename}', downloaded.write)
#         return True
#     except ftplib.error_perm:
#         content  = f"Missing file {filename}. Please can you upload the file as soon as possible?"
#         subject=  f"Missing file {filename}."
#         send_email(host= smtp_host, port= smtp_port, user = smtp_user, receivers = receivers, subject = subject, content=content) 
#         return False

def file_exists(hostname: str, user: str, password: str, date: str, smtp_host, smtp_user, smtp_port, receivers)-> bool:
    """
    """
    filename = f"extract_vbm_{date.replace('-', '')}.csv"
    logging.info(f"Reading {filename} ")
    try:
        print(hostname)
        print(user)
        print(password)
        print(date)
        server = ftplib.FTP(hostname, user, password, timeout=200)
    except Exception as error :
        content  = f"Authentication error to FTP server"
        subject=  f"Authentication error."
        send_email(host= smtp_host, port= smtp_port, user = smtp_user, receivers = receivers, subject = subject, content=content) 
        return False
    logging.info("check file")
    server.cwd(CONFIG["ftp_dir"])
    file_list = server.nlst()
    if filename not in file_list:
        content  = f"Missing file {filename}. Please can you upload the file as soon as possible?"
        subject=  f"Missing file {filename}."
        send_email(host= smtp_host, port= smtp_port, user = smtp_user, receivers = receivers, subject = subject, content=content)
    return filename in file_list

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
    # 
    
    # try:
    #     logging.info("downloading....")
    #     server.retrbinary(f'RETR {filename}', downloaded.write)
    # except ftplib.error_perm as error:
    #     raise OSError(f"{filename} don't exists on FTP server") from error

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
    #Read the downloaded file into a DataFrame
    downloaded.seek(0)
    try:
        logging.info("Read data")
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
    df_.columns = df_.columns.str.lower()
    df_.columns = [unidecode(col) for col in df_.columns]
    good_columns = [d["columns"]
                    for d in CONFIG["tables"] if d["name"] == "caparc"][0]
    missing_columns = set(good_columns).difference(set(df_.columns))
    if missing_columns:
        raise ValueError(f"missing columns {', '.join(missing_columns)}")
    # Add "month_id" columns
    try:
        logging.info("add column")
        df_["day_id"] = df_["day_id"].astype(str)
        df_["month_id"] = df_["day_id"].str[:4].str.cat(
            df_["day_id"].str[4:6], "-")
    except Exception as error:
        raise ValueError("Failed to add the 'month_id' column.") from error
    return df_
