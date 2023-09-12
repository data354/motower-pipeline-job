import pandas as pd
from datetime import timedelta, datetime
import psycopg2
import logging
from dateutil import relativedelta
from gps import CONFIG
from gps.common.rwminio import get_latest_file, save_minio




def cleaning_daily_trafic(client, endpoint: str, accesskey: str, secretkey: str, date: str):
    """
    Cleans traffic files
    Args:
        - client: Minio client
        - endpoint: Minio endpoint
        - accesskey: Minio access key
        - secretkey: Minio secret key
        - date: execution date (provided by airflow %Y-%m-%d)
    Return:
        None
    """
     # Find the required object in the CONFIG dictionary
    objet = next((table for table in CONFIG["tables"] if table["name"] == "ks_daily_tdb_radio_drsi"), None)
    if not objet:
        raise ValueError("Table ks_daily_tdb_radio_drsi not found.")
     # Check if bucket exists
    if not client.bucket_exists(objet["bucket"]):
        raise ValueError(f"Bucket {objet['bucket']} does not exist.")
     # Split the date into parts
    date_parts = date.split("-")
    filename = get_latest_file(client, objet["bucket"], prefix = f"{objet['folder']}/{date_parts[0]}/{date_parts[1]}/{date_parts[2]}")
    try:
        logging.info("read %s", filename)
        trafic = pd.read_csv(f"s3://{objet['bucket']}/{filename}",
                                storage_options={
                                "key": accesskey,
                                "secret": secretkey,
                "client_kwargs": {"endpoint_url": f"http://{endpoint}"}
                }
                    )
    except Exception as error:
        raise OSError(f"{filename} don't exists in bucket") from error
    trafic.columns = trafic.columns.str.lower()
    trafic = trafic.loc[trafic["techno"].isin(["2G", "3G", "4G"]), :]
    try:
        trafic["trafic_data_go"] = trafic["trafic_data_go"].astype("float")
        trafic["trafic_voix_erl"] = trafic["trafic_voix_erl"].astype("float")
    except AttributeError :
        trafic["trafic_data_go"] = trafic["trafic_data_go"].str.replace(",", ".").astype("float")
        trafic["trafic_voix_erl"] = trafic["trafic_voix_erl"].str.replace(",", ".").astype("float")

    trafic = trafic[["date_id", "id_site", "trafic_data_go", "trafic_voix_erl", "techno" ]]
    trafic = trafic.groupby(["date_id", "id_site", "techno"]).sum()
    trafic = trafic.unstack()
    trafic.columns = ["_".join(d) for d in trafic.columns]
    trafic.reset_index(drop=False, inplace=True)
    trafic.columns = ["jour", "id_site", "trafic_data_2g", "trafic_data_3g", "trafic_data_4g", "trafic_voix_2g", "trafic_voix_3g", "trafic_voix_4g" ]
     # Save the cleaned dataFrame to Minio
    return trafic
    
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
    exec_week = exec_date.isocalendar()[1]
    exec_year = exec_date.isocalendar()[0]
    if exec_date >= datetime(2023, 7, 3):
        
    # get   congestion 
        logging.info("get congestion")
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
        
        logging.info("get  trafic")
        objet = next((table for table in CONFIG["tables"] if table["name"] == "ks_daily_tdb_radio_drsi"), None)
        if not objet:
            raise ValueError("Table ks_daily_tdb_radio_drsi not found.")
        # Check if bucket exists
        if not client.bucket_exists(objet["bucket"]):
            raise ValueError(f"Bucket {objet['bucket']} does not exist.")
        # Split the date into parts
        date_parts = thedate.split("-")
        filename = get_latest_file(client, objet["bucket"], prefix = f"{objet['folder']}-cleaned/{date_parts[0]}/{date_parts[1]}/{date_parts[2]}")
        logging.info(f"reading {filename}")
        try:
            trafic = pd.read_csv(f"s3://{objet['bucket']}/{filename}",
                            storage_options={
                                "key": accesskey,
                                "secret": secretkey,
                                "client_kwargs": {"endpoint_url": f"http://{endpoint}"}
                            })
        except Exception as error:
            raise ValueError(f"{filename} does not exist in bucket.") from error
        


        print(trafic.head())
        # get daily data
        # start = datetime.strptime(thedate, "%Y-%m-%d") - timedelta(days=7)
        logging.info("GET LAST WEEK DAILY DATA")
        
        
        conn = psycopg2.connect(host=pghost, database=pgdb, user=pguser, password=pgpwd)
        sql_query =  "select * from motower_daily where EXTRACT(WEEK FROM jour) = %s and EXTRACT(YEAR FROM jour) = %s "
        daily_week_df = pd.read_sql_query(sql_query, conn, params=(str(exec_week), str(exec_year)))
        print(congestion.shape)
        print(daily_week_df.shape)
        daily_week_df["code_oci_id"] = daily_week_df["code_oci_id"].astype("float")
        # daily_week_df['code_oci_id'] = daily_week_df["code_oci"].str.replace('OCI', '')
        logging.info("MERGE DATA")
        # merge data
        congestion["id_site"] = congestion["id_site"].astype("float")
        trafic["id_site"] = trafic["id_site"].astype("float")
        weekly = daily_week_df.merge(congestion, left_on =["code_oci_id"], right_on = ["id_site"], how="left")
        print(weekly.shape)
        weekly = weekly.drop(columns=["jour_y"])
        weekly.rename(columns={"jour_x":"jour"}, inplace=True)

        weekly_f = weekly.merge(trafic, left_on =["jour","code_oci_id" ], right_on = ["jour","id_site"], how="left")
        print(weekly_f.loc[0:20, ["code_oci_id", "id_site_x", "id_site_y" ,"jour","trafic_data_2g"]])
        weekly_f = weekly_f.drop(columns=["id_site_y"])
        weekly_f.rename(columns={"id_site_x":"id_site"}, inplace=True)
        weekly_f = weekly_f.drop(columns=["id"])
        print(weekly_f.shape)
        print(weekly_f.loc[0:20, ["code_oci_id", "id_site", "jour","trafic_data_2g"]])
        return weekly_f
    else:
        conn = psycopg2.connect(host=pghost, database=pgdb, user=pguser, password=pgpwd)
        sql_query =  "select * from motower_daily where EXTRACT(WEEK FROM jour) = %s and EXTRACT(YEAR FROM jour) = %s "
        daily_week_df = pd.read_sql_query(sql_query, conn, params=(str(exec_week), str(exec_year)))
        print(daily_week_df.shape)
        daily_week_df["code_oci_id"] = daily_week_df["code_oci_id"].astype("float")
        #daily_week_df['code_oci_id'] = daily_week_df["code_oci"].str.replace('OCI', '')

        logging.info("get  trafic")
        objet = next((table for table in CONFIG["tables"] if table["name"] == "ks_daily_tdb_radio_drsi"), None)
        if not objet:
            raise ValueError("Table ks_daily_tdb_radio_drsi not found.")
        # Check if bucket exists
        if not client.bucket_exists(objet["bucket"]):
            raise ValueError(f"Bucket {objet['bucket']} does not exist.")
        # Split the date into parts
        date_parts = thedate.split("-")
        filename = get_latest_file(client, objet["bucket"], prefix = f"{objet['folder']}-cleaned/{date_parts[0]}/{date_parts[1]}/{date_parts[2]}")
        logging.info(f"reading {filename}")
        try:
            trafic = pd.read_csv(f"s3://{objet['bucket']}/{filename}",
                            storage_options={
                                "key": accesskey,
                                "secret": secretkey,
                                "client_kwargs": {"endpoint_url": f"http://{endpoint}"}
                            })
        except Exception as error:
            raise ValueError(f"{filename} does not exist in bucket.") from error
        print(trafic.shape)
        print(trafic["jour"].unique())
        trafic["id_site"] = trafic["id_site"].astype("float")
        #  MERGE DATA
        print(daily_week_df["code_oci_id"].unique()[0:5])
        print(trafic["id_site"].unique()[0:5])
        weekly_f = daily_week_df.merge(trafic, left_on =["jour","code_oci_id"], right_on = ["jour","id_site"], how="left")
        #weekly_f = weekly_f.drop(columns=["jour_y"])
        #weekly_f.rename(columns={"jour_x":"jour"}, inplace=True)
        weekly_f = weekly_f.drop(columns=["id"])
        print(weekly_f.columns)

        return weekly_f
    

