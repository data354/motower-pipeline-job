import pandas as pd
from datetime import timedelta, datetime
import psycopg2
import logging
from dateutil import relativedelta
from gps import CONFIG
from gps.common.rwminio import get_latest_file
from gps.common.extract import get_connection




# def get_trafic(host: str, database: str, user: str, password: str, date):
#     """
#     """

#     exec_week = date.isocalendar()[1]
#     conn = get_connection(host, database, user, password)
#     sql_query = '''select * from "ENERGIE"."KS_DAILY_TDB_RADIO_DRSI" where EXTRACT(WEEK FROM "DATE_ID") = %d AND EXTRACT(YEAR FROM "DATE_ID" ) = %d'''
#     logging.info(f"get data to date {date}")
#     df_ = pd.read_sql_query(sql_query, conn, params=(exec_week,))


def cleaning_congestion(client, endpoint: str, accesskey: str, secretkey: str, date: str):
    """
    Cleans traffic files
    Args:
        - client: Minio client
        - endpoint: Minio endpoint
        - accesskey: Minio access key
        - secretkey: Minio secret key
        - date: execution date (provided by airflow)
    Return:
        None
    """
     # Find the required object in the CONFIG dictionary
    objet = next((table for table in CONFIG["tables"] if table["name"] == "ks_hebdo_tdb_radio_drsi"), None)
    if not objet:
        raise ValueError("Table ks_hebdo_tdb_radio_drsi not found.")
     # Check if bucket exists
    if not client.bucket_exists(objet["bucket"]):
        raise ValueError(f"Bucket {objet['bucket']} does not exist.")
     # Split the date into parts
    date_parts = date.split("-")
    filename = get_latest_file(client, objet["bucket"], prefix = f"{objet['folder']}/{date_parts[0]}/{date_parts[1]}/{date_parts[2]}")
    try:
        df_ = pd.read_csv(f"s3://{objet['bucket']}/{filename}",
                           storage_options={
                               "key": accesskey,
                               "secret": secretkey,
                               "client_kwargs": {"endpoint_url": f"http://{endpoint}"}
                           })
    except Exception as error:
        raise ValueError(f"{filename} does not exist in bucket.") from error
    df_.columns = df_.columns.str.lower()
    df_ = df_.loc[:,["date_id", "id_site", "nbre_cellule", "nbre_cellule_congestionne", "techno"]]
    df_.columns = ["jour", "id_site", "cellules", "cellules_congestionnees", "techno"]
    df_ = df_.loc[df_.techno != "TDD", :]
    df_ = df_.groupby(["jour",	"id_site","techno"	]).sum()
    df_ = df_.unstack()
    df_.columns = ["_".join(d) for d in df_.columns]
    
    df_ = df_.reset_index(drop=False)
    df_.columns = ["jour", "id_site", "cellules_2g", "cellules_3g", "cellules_4g", "cellules_2g_congestionnees", "cellules_3g_congestionnees", "cellules_4g_congestionnees"]
    df_["cellules_totales"] = df_["cellules_2g"] + df_["cellules_3g"] + df_["cellules_4g"]
    df_["cellules_congestionnees_totales"] = df_["cellules_2g_congestionnees"] + df_["cellules_3g_congestionnees"] + df_["cellules_4g_congestionnees"]
    
    return df_



def motower_weekly(client, endpoint: str, accesskey: str, secretkey: str, thedate: str, pghost, pguser, pgpwd, pgdb):
    """
    """
    exec_date = datetime.strptime(thedate, "%Y-%m-%d") 
    if exec_date >= datetime(2023, 7, 3):
        
    # get   congestion 
        objet = next((table for table in CONFIG["tables"] if table["name"] == "ks_hebdo_tdb_radio_drsi"), None)
        if not objet:
            raise ValueError("Table ks_hebdo_tdb_radio_drsi not found.")
        # Check if bucket exists
        if not client.bucket_exists(objet["bucket"]):
            raise ValueError(f"Bucket {objet['bucket']} does not exist.")
        # Split the date into parts
        date_parts = thedate.split("-")
        filename = get_latest_file(client, objet["bucket"], prefix = f"{objet['folder']}-cleaned/{date_parts[0]}/{date_parts[1]}/{date_parts[2]}")
        logging.info(f"reading {filename}")
        try:
            congestion = pd.read_csv(f"s3://{objet['bucket']}/{filename}",
                            storage_options={
                                "key": accesskey,
                                "secret": secretkey,
                                "client_kwargs": {"endpoint_url": f"http://{endpoint}"}
                            })
        except Exception as error:
            raise ValueError(f"{filename} does not exist in bucket.") from error
        
        # get daily data
        # start = datetime.strptime(thedate, "%Y-%m-%d") - timedelta(days=7)
        logging.info("GET LAST WEEK DAILY DATA")
        
        exec_week = exec_date.isocalendar()[1]
        exec_year = exec_date.isocalendar()[0]
        print(exec_week)
        conn = psycopg2.connect(host=pghost, database=pgdb, user=pguser, password=pgpwd)
        sql_query =  "select * from motower_daily where EXTRACT(WEEK FROM jour) = %s and EXTRACT(YEAR FROM jour) = %s "
        daily_week_df = pd.read_sql_query(sql_query, conn, params=(str(exec_week), str(exec_year)))
        print(congestion.shape)
        print(daily_week_df.shape)
        daily_week_df["code_oci"] = daily_week_df["code_oci"].astype("str")
        daily_week_df['code_oci_id'] = daily_week_df["code_oci"].str.replace('OCI', '')
        logging.info("MERGE DATA")
        # merge data
        congestion["id_site"] = congestion["id_site"].astype("str")
        weekly = daily_week_df.merge(congestion, left_on =["code_oci_id"], right_on = ["id_site"], how="left")
        print(weekly.shape)
        weekly = weekly.drop(columns=["jour_y"])
        weekly.rename(columns={"jour_x":"jour"}, inplace=True)
        weekly["trafic_data_in"] = weekly["trafic_data_in"] / 1000

        weekly = weekly.drop(columns=["id"])
        return weekly
    else:
        conn = psycopg2.connect(host=pghost, database=pgdb, user=pguser, password=pgpwd)
        sql_query =  "select * from motower_daily where EXTRACT(WEEK FROM jour) = %s and EXTRACT(YEAR FROM jour) = %s "
        daily_week_df = pd.read_sql_query(sql_query, conn, params=(str(exec_week), str(exec_year)))
        print(daily_week_df.shape)
        daily_week_df["code_oci"] = daily_week_df["code_oci"].astype("str")
        daily_week_df['code_oci_id'] = daily_week_df["code_oci"].str.replace('OCI', '')
        return daily_week_df
    

