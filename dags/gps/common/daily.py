import logging
import pandas as pd
import psycopg2
from datetime import datetime
from dateutil import relativedelta
from gps import CONFIG
from gps.common.rwminio import  get_latest_file, save_minio




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

    
    
    
def motower_daily(client, endpoint: str, accesskey: str, secretkey: str, date: str, pghost, pguser, pgpwd, pgdb):
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
    logging.info("Reading %s", filename)
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
    
    
    logging.info(f"get caparc of the day {date}")
    table_obj = next((table for table in CONFIG["tables"] if table["name"] == "caparc"), None)
    date_parts = date.split("-")
    filename = f"{table_obj['folder']}/{date_parts[0]}/{date_parts[1]}/{date_parts[2]}.csv"
    logging.info("Reading %s", filename)
    try:
        ca = pd.read_csv(f"s3://{table_obj['bucket']}/{filename}",
                           storage_options={
                               "key": accesskey,
                               "secret": secretkey,
                               "client_kwargs": {"endpoint_url": f"http://{endpoint}"}
                           })
    except Exception as error:
        raise ValueError(f"{filename} does not exist in bucket.") from error


    logging.info("merge bdd and CA")
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
    
    # df_final["trafic_data_total"] = df_final["trafic_data_2g"] + df_final["trafic_data_3g"] + df_final["trafic_data_4g"]
    # df_final["trafic_voix_total"] = df_final["trafic_voix_2g"] + df_final["trafic_voix_3g"] + df_final["trafic_voix_4g"]

    # INIT COLUMNS
    df_final["ca_norm"] = None
    df_final["ca_mtd"] = None
    df_final["segment"] = None
    df_final["previous_segment"] = None

    # GET DATA MONTH TO DAY
    if exec_date.day == 1:
        for idx, row in df_final.iterrows():
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
        sql_query =  "select * from motower_daily where EXTRACT(MONTH FROM jour) = %s AND jour < %s"
        daily_month_df = pd.read_sql_query(sql_query, conn, params=(mois, date))
        
        logging.info("ADD CA_MTD AND SEGMENT")
        if daily_month_df.shape[0]>0:
            month_data = pd.concat([daily_month_df, df_final])
            for idx, row in df_final.iterrows():
                code_oci = row["code_oci"]
                date_row = row["jour"]
                loc_row = row["localisation"]
                mtd_rows = month_data.loc[month_data["code_oci"] == code_oci, :]
                ca_mtd = mtd_rows["ca_total"].sum()
                ca_norm = ca_mtd * 30 / date_row.day
                segment = compute_segment(ca_norm, loc_row)
                df_final.loc[idx, ["ca_mtd", "ca_norm", "segment"]] = [ca_mtd, ca_norm, segment]
    
        logging.info("ADD PREVIOUS SEGMENT")
        lmonth = exec_date - relativedelta.relativedelta(months=1)
        if lmonth!=6:
            sql_query =  "select * from motower_weekly where  EXTRACT(MONTH FROM jour) = %s and EXTRACT(DAY FROM jour) = %s "
            last_month = pd.read_sql_query(sql_query, conn, params=(lmonth.month,exec_date.day))
            if last_month.shape[0] > 0:
                for idx, row in df_final.iterrows():
                    code_oci = row["code_oci"]
                    date_row = row["jour"]
                    previos_segment = last_month.loc[last_month.code_oci==code_oci, "segment"].values[0]
                    print(previos_segment)
                    df_final.loc[idx, "previous_segment"] = previos_segment
    return df_final
    