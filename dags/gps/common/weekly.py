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


def compute_segment(ca:float, loc:str)->str:
    segment = None
    if not ca or not loc :
        return segment
    loc = loc.lower()
    if loc=='abidjan':
        segment = "PREMIUM" if ca>=20000000 else "NORMAL" if ca>=10000000 else "A DEVELOPER"
    if loc=='intérieur':
        segment = "PREMIUM" if ca>=10000000 else "NORMAL" if ca>=4000000 else "A DEVELOPER"
    return segment



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
    exec_date = datetime.strptime(thedate, "%Y-%m-%d") - timedelta(days=1)
    exec_week = exec_date.isocalendar()[1]
    conn = psycopg2.connect(host=pghost, database=pgdb, user=pguser, password=pgpwd)
    sql_query =  "select * from motower_daily where EXTRACT(MONTH FROM jour) = %s AND jour <= %s"
    mois = int(thedate.split('-')[1])
    daily_month_df = pd.read_sql_query(sql_query, conn, params=(mois, thedate))
    daily_month_df["code_oci"] = daily_month_df["code_oci"].astype("str")
    daily_month_df['code_oci_id'] = daily_month_df["code_oci"].str.replace('OCI', '')
    print(daily_month_df.info())
    idx = pd.to_datetime(arg=daily_month_df["jour"], format="%Y-%m-%d").dt.week == exec_week
    daily_week_df = daily_month_df[idx]
    #
    # merge data
    congestion["id_site"] = congestion["id_site"].astype("str")
    weekly = daily_week_df.merge(congestion, left_on =["code_oci_id"], right_on = ["id_site"], how="left")
    weekly = weekly.drop(columns=["jour_y"])
    weekly.rename(columns={"jour_x":"jour"}, inplace=True)
    

    # add CA MTD AND SEGMENT
    weekly["ca_mtd"] = None
    weekly["ca_norm"] = None
    weekly["segment"] = None
    for idx, row in weekly.iterrows():
        code_oci = row["code_oci"]
        date_row = row["jour"]
        loc_row = row["localisation"]
        mtd_rows = daily_month_df.loc[(weekly["code_oci"] == code_oci) & (weekly["jour"] == date_row), :]
        ca_mtd = mtd_rows["ca_total"].sum()
        ca_norm = ca_mtd * 30 / date_row.day
        segment = compute_segment(ca_norm, loc_row)
        weekly.loc[idx, ["ca_mtd", "ca_norm", "segment"]] = [ca_mtd, ca_norm, segment]


    
    weekly["trafic_data_in"] = weekly["trafic_data_in"] / 1000

    #add segment
    print(weekly.columns)
    lmonth = (datetime.strptime(thedate, "%Y-%m-%d") - relativedelta.relativedelta(months=1)).month
    if lmonth!=6:
        sql_query =  "select * from motower_weekly where  EXTRACT(MONTH FROM jour) = %s"
        last_month = pd.read_sql_query(sql_query, conn, params=(lmonth,))

        weekly["previous_segment"] = None
        if last_month.shape[0] > 0:
            for idx, row in weekly.iterrows():
                previos_segment = last_month.loc[(last_month.code_oci==row["code_oci"]) & (last_month["jour"].dt.day == str(row["jour"].day)), "segment"].values
                weekly.loc[idx, "previous_segment"] = previos_segment
    weekly = weekly.drop(columns=["id"])
    return weekly

