import logging
import math
import pandas as pd
import psycopg2
from datetime import datetime
from dateutil import relativedelta
from gps import CONFIG
from gps.common.rwminio import  get_latest_file
from gps.common.data_validation import validate_column, validate_site_actifs




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


    
def generate_daily_caparc(client, endpoint: str, accesskey: str, secretkey: str, date: str, pghost, pguser, pgpwd, pgdb):
    """
        create motower daily structure
    """

    logging.info("get last modified bdd sites cleaned")
    exec_date = datetime.strptime(date, CONFIG["date_format"])
    table_obj = next((table for table in CONFIG["tables"] if table["name"] == "BASE_SITES"), None)
    if not table_obj:
        raise ValueError("Table BASE_SITES not found.")
     # Check if bucket exists
    if not client.bucket_exists(table_obj["bucket"]):
        raise ValueError(f"Bucket {table_obj['bucket']} does not exist.")
     # Get filename
    filename = get_latest_file(client=client, bucket=table_obj["bucket"], prefix=f"{table_obj['folder']}-cleaned/")
    logging.info("READING %s", filename)
     # Read file from minio
    try:
        bdd = pd.read_csv(f"s3://{table_obj['bucket']}/{filename}",
                           storage_options={
                               "key": accesskey,
                               "secret": secretkey,
                               "client_kwargs": {"endpoint_url": f"http://{endpoint}"}
                           })
    except Exception as error:
        raise ValueError(f"{filename} does not exist in bucket.") from error
    
    # BDD SITE VALIDATION
    validate_site_actifs(bdd, col="code oci")

    # CA PARC
    logging.info(f"GET CA PARC FILE OF THE DAY {date}")
    table_obj = next((table for table in CONFIG["tables"] if table["name"] == "caparc"), None)
    date_parts = date.split("-")
    filename = f"{table_obj['folder']}/{date_parts[0]}/{date_parts[1]}/{date_parts[2]}.csv"
    logging.info("READING %s", filename)
    try:
        ca = pd.read_csv(f"s3://{table_obj['bucket']}/{filename}",
                           storage_options={
                               "key": accesskey,
                               "secret": secretkey,
                               "client_kwargs": {"endpoint_url": f"http://{endpoint}"}
                           })
    except Exception as error:
        raise ValueError(f"{filename} does not exist in bucket.") from error
    
    # CAPARC VALIDATION
    df_for_validation = ca.groupby("day_id").aggregate({'ca_voix': 'sum', 'ca_data': 'sum','parc': 'sum', 'parc_data': 'sum', "parc_2g": 'sum',
          "parc_3g": 'sum',
          "parc_4g": 'sum',
          "parc_5g": 'sum',
          "parc_other": 'sum', 
          "ca_total": 'sum'})
    df_for_validation.reset_index(drop=False, inplace=True)

    logging.info('DAILY KPI - {}'.format(df_for_validation.to_string()))
    for col in ["ca_total","ca_voix", "ca_data", "parc", "parc_data"]:
        validate_column(df_for_validation, col, date=date)

    logging.info("MERGING BDD AND CA")
    bdd_ca = bdd.merge(ca, left_on=["code oci id"], right_on = ["id_site" ], how="left")
   
    logging.info("prepare final daily data")
    
    bdd_ca["jour"] = exec_date

    df_final = bdd_ca.loc[:,["jour", "code oci", "code oci id", "autre code", "clutter", "commune", "departement", "type du site", "type geolocalite", "gestionnaire",
                           "latitude", "longitude", "localisation", "partenaires", "proprietaire", "position site", "site", "statut", "projet", "region",
                           "ca_data", "ca_voix", "ca_total", "parc", "parc_data", "parc_2g", "parc_3g", "parc_4g", "parc_5g", "parc_other", "trafic_data_in",
                           "trafic_voix_in"]]
    
    df_final.columns = ["jour", "code_oci","code_oci_id", "autre_code", "clutter", "commune", "departement", "type_du_site", "type_geolocalite", "gestionnaire",
                           "latitude", "longitude", "localisation", "partenaires", "proprietaire", "position_site", "site", "statut", "projet", "region",
                           "ca_data", "ca_voix", "ca_total", "parc_global", "parc_data", "parc_2g", "parc_3g", "parc_4g", "parc_5g", "autre_parc", "trafic_data_in",
                           "trafic_voix_in"]
    
    df_final["trafic_data_in_mo"] = df_final["trafic_data_in"]  / 1000
    
    # INIT COLUMNS
    df_final["ca_norm"] = None
    df_final["ca_mtd"] = None
    df_final["segment"] = None
    df_final["previous_segment"] = None

    # GET DATA MONTH TO DAY
    if exec_date.day == 1:
        for idx, row in df_final.loc[df_final["localisation"].notna(),:].iterrows():
                code_oci = row["code_oci"]
                date_row = row["jour"]
                loc_row = row["localisation"]
                ca_mtd = row["ca_total"]
                ca_norm = ca_mtd * 30 
                segment = compute_segment(ca_norm, loc_row)
                df_final.loc[idx, ["ca_mtd", "ca_norm", "segment"]] = [ca_mtd, ca_norm, segment]
        
                
    if exec_date.day > 1:
        logging.info("GET DATA MONTH TO DAY")
        
        mois = exec_date.month
        conn = psycopg2.connect(host=pghost, database=pgdb, user=pguser, password=pgpwd)
        sql_query =  "select * from motower_daily_caparc where EXTRACT(MONTH FROM jour) = %s AND jour < %s"
        daily_month_df = pd.read_sql_query(sql_query, conn, params=(mois, date))
        
        logging.info("ADD CA_MTD AND SEGMENT")
        if daily_month_df.shape[0]>0:
            month_data = pd.concat([daily_month_df, df_final])
            for idx, row in df_final.loc[df_final["localisation"].notna(),:].iterrows():
                code_oci = row["code_oci"]
                date_row = row["jour"]
                loc_row = row["localisation"]
                mtd_rows = month_data.loc[month_data["code_oci"] == code_oci, :]
                ca_mtd = mtd_rows["ca_total"].sum()
                ca_norm = ca_mtd * 30 / date_row.day
                print(loc_row)         
                segment = compute_segment(ca_norm, loc_row)
                df_final.loc[idx, ["ca_mtd", "ca_norm", "segment"]] = [ca_mtd, ca_norm, segment]

        logging.info("ADD PREVIOUS SEGMENT")
        lmonth = exec_date - relativedelta.relativedelta(months=1)
        if lmonth!=6:
            sql_query =  "select * from motower_daily_caparc where  EXTRACT(MONTH FROM jour) = %s and EXTRACT(DAY FROM jour) = %s "
            last_month = pd.read_sql_query(sql_query, conn, params=(lmonth.month,exec_date.day))
            if last_month.shape[0] > 0:
                for idx, row in df_final.iterrows():
                    code_oci = row["code_oci"]
                    date_row = row["jour"]
                    previos_segment = last_month.loc[last_month.code_oci==code_oci, "segment"].values[0]
                    print(previos_segment)
                    df_final.loc[idx, "previous_segment"] = previos_segment

    logging.info("DATA VALIDATION AFTER MERGING")
    validate_site_actifs(df_final, col = "code_oci")
    logging.info(f"NUMBER OF ACTIFS SITES WITH CA IS {df_final.loc[df_final['ca_total'].notnull(),:].shape[0]}")
    df_for_validation = df_final.groupby("jour").aggregate({'ca_voix': 'sum', 'ca_data': 'sum','parc_global': 'sum', 'parc_data': 'sum', "parc_2g": 'sum',
          "parc_3g": 'sum',
          "parc_4g": 'sum',
          "parc_5g": 'sum',
          "autre_parc": 'sum', 
          "ca_total": 'sum'})
    df_for_validation.reset_index(drop=False, inplace=True)

    logging.info('DAILY KPI - {}'.format(df_for_validation.to_string()))
    for col in ["ca_total","ca_voix", "ca_data", "parc_global", "parc_data"]:
        validate_column(df_for_validation, col, date=date)
    
    return df_final


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
    
    # DATA VALIDATION
    df_to_validate = trafic.groupby("date_id").aggregate({'trafic_data_go': 'sum', 'trafic_voix_erl': 'sum'})
    df_to_validate.reset_index(drop=False, inplace=True)
    for col in ["trafic_data_go", "trafic_voix_erl"]:
        validate_column(df_to_validate, col, date=df_to_validate["date_id"].unique()[0])

    logging.info("prepare final data")
    trafic = trafic[["date_id", "id_site", "trafic_data_go", "trafic_voix_erl", "techno" ]]
    trafic = trafic.groupby(["date_id", "id_site", "techno"]).sum()
    trafic = trafic.unstack()
    trafic.columns = ["_".join(d) for d in trafic.columns]
    trafic.reset_index(drop=False, inplace=True)
    trafic.columns = ["jour", "id_site", "trafic_data_2g", "trafic_data_3g", "trafic_data_4g", "trafic_voix_2g", "trafic_voix_3g", "trafic_voix_4g" ]
     # Save the cleaned dataFrame to Minio
    trafic["trafic_voix_total"] = trafic["trafic_voix_2g"] + trafic["trafic_voix_3g"] + trafic["trafic_voix_4g"]
    trafic["trafic_data_total"] = trafic["trafic_data_2g"] + trafic["trafic_data_3g"] + trafic["trafic_data_4g"]
    trafic["id_site"] = trafic["id_site"].astype("float")
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
    exec_date = datetime.strptime(date, CONFIG["date_format"])
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
    df_["date_id"] = exec_date
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
    df_["id_site"] = df_["id_site"].astype("float")
    return df_

    