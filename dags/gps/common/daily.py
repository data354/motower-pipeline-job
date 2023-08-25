import logging
import pandas as pd
from gps import CONFIG
from gps.common.rwminio import  get_latest_file


def motower_daily(client, endpoint: str, accesskey: str, secretkey: str, date: str):


    logging.info("get last modified bdd sites cleaned")
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
    bdd_ca["jour"] = date
    df_final = bdd_ca.loc[:,["jour", "code oci", "code oci id", "autre code", "clutter", "commune", "departement", "type du site", "type geolocalite", "gestionnaire",
                           "latitude", "longitude", "localisation", "partenaires", "proprietaire", "position site", "site", "statut", "projet", "region",
                           "ca_data", "ca_voix", "ca_total", "parc", "parc_data", "parc_2g", "parc_3g", "parc_4g", "parc_5g", "parc_other", "trafic_data_in",
                           "trafic_voix_in"]]
    df_final.columns = ["jour", "code_oci","code_oci_id", "autre_code", "clutter", "commune", "departement", "type_du_site", "type_geolocalite", "gestionnaire",
                           "latitude", "longitude", "localisation", "partenaires", "proprietaire", "position_site", "site", "statut", "projet", "region",
                           "ca_data", "ca_voix", "ca_total", "parc_global", "parc_data", "parc_2g", "parc_3g", "parc_4g", "parc_5g", "autre_parc", "trafic_data_in_ko",
                           "trafic_voix_in_minute"]
    
    return df_final
    