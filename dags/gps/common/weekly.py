import pandas as pd
from datetime import timedelta, datetime
import psycopg2
import logging
from dateutil import relativedelta
from gps import CONFIG
from gps.common.rwminio import get_latest_file

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
    # end = datetime.strptime(thedate, "%Y-%m-%d") - timedelta(days=1)
    conn = psycopg2.connect(host=pghost, database=pgdb, user=pguser, password=pgpwd)
    sql_query =  "select * from motower_daily where EXTRACT(MONTH FROM jour) = %s "
    mois = int(thedate.split('-')[1])
    daily = pd.read_sql_query(sql_query, conn, params=(mois,))
    # merge data
    congestion["id_site"] = congestion["id_site"].astype("str")
    daily["code_oci"] = daily["code_oci"].astype("str")
    daily['code_oci_id'] = daily["code_oci"].str.replace('OCI', '')
    weekly = daily.merge(congestion, left_on =["code_oci_id"], right_on = ["id_site"], how="left")
    weekly = weekly.drop(columns=["jour_y"])
    weekly.rename(columns={"jour_x":"jour"}, inplace=True)
    print(weekly.columns)
    

    # add CA MTD
    dayofmonth = int(thedate.split("-")[-1])
    weekly_i = weekly.groupby(["code_oci"]).agg(ca_sum = ('ca_total', 'sum'))
    weekly_i = weekly_i.reset_index(drop=False)
    weekly_i["ca_mtd"] = weekly_i["ca_sum"] * 30 / dayofmonth
    weekly_i["jour"] = thedate

    weekly_f = weekly.merge(weekly_i, left_on =["code_oci", "jour"], right_on = ["code_oci", "jour"], how="left")
    #add segment
    weekly_f.loc[((weekly_f.localisation.str.lower()=="abidjan") & (weekly_f.ca_mtd>=20000000)) | ((weekly_f.localisation.str.lower()=="intérieur") & (weekly_f.ca_mtd>=10000000)),["segment"]] = "PREMIUM"
    weekly_f.loc[((weekly_f.localisation.str.lower()=="abidjan") & ((weekly_f.ca_mtd>=10000000) & (weekly_f.ca_mtd<20000000) )) | ((weekly_f.localisation.str.lower()=="intérieur") & ((weekly_f.ca_mtd>=4000000) & (weekly_f.ca_mtd<10000000))),["segment"]] = "NORMAL"
    weekly_f.loc[((weekly_f.localisation.str.lower()=="abidjan") & (weekly_f.ca_mtd<10000000)) | ((weekly_f.localisation.str.lower()=="intérieur") & (weekly_f.ca_mtd<4000000)),["segment"]] = "A DEVELOPPER"
    weekly_f["trafic_data_in"] = weekly_f["trafic_data_in"] / 1000
    print(weekly_f.columns)
    weekly_f.drop(columns=["ca_sum"], inplace=True)

    lmonth = (datetime.strptime(thedate, "%Y-%m-%d") - relativedelta.relativedelta(months=1)).month
    if lmonth!=6:
        sql_query =  "select * from motower_weekly where  EXTRACT(MONTH FROM jour) = %s"
        last_month = pd.read_sql_query(sql_query, conn, params=(lmonth,))

        weekly_f["previous_segment"] = None
        if last_month.shape[0] > 0:
            for idx, row in weekly_f.iterrows():
                previos_segment = last_month.loc[(last_month.code_oci==row["code_oci"]) & (last_month["jour"].dt.day == str(row["jour"].day)), "segment"].values
                weekly_f.loc[idx, "previous_segment"] = previos_segment
    return weekly_f