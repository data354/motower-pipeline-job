from gps import CONFIG
from pathlib import Path
import pandas as pd
from minio import Minio
from gps.common.rwminio import save_minio 
from pandas.io.excel import ExcelFile

import logging

def cleaning_base_site(endpoint:str, accesskey:str, secretkey:str,  date: str)-> None:
    """
     function clean data from minio
    """

    client = Minio(
        endpoint,
        access_key= accesskey,
        secret_key= secretkey,
        secure=False)
    objet = [d for d in CONFIG["tables"] if d["name"] == "BASE_SITES"][0]
    if not client.bucket_exists(objet["bucket"]):
        raise OSError(f"bucket {objet['bucket']} don\'t exits")
    filename = f"BASE_SITES_{date.split('-')[0]}{date.split('-')[1]}.xlsx"
    try:
        logging.info("read %s", filename)
        df = pd.read_excel(f"s3://{objet['bucket']}/{objet['folder']}/{filename}",
            storage_options={
            "key": accesskey,
            "secret": secretkey,
            "client_kwargs": {"endpoint_url": f"http://{endpoint}"}
            }
                )
    except Exception as error:
        raise OSError(f"{filename} don't exists in bucket") from error
        

    # check columns
    logging.info("check columns")
    df.columns = df.columns.str.lower()
    missing_columns = set(objet["columns"]).difference(set(df.columns))
    if len(missing_columns):
        raise ValueError(f"missing columns {', '.join(missing_columns)}")
    logging.info("columns are ok")
   
    # strip columns
    logging.info("clean ans enrich")
    cols_to_trim = ["code oci", "autre code"]
    df[objet["columns"]] = df[objet["columns"]].apply(lambda x: x.astype("str"))
    df[cols_to_trim] = df[cols_to_trim].apply(lambda x: x.str.strip())
    df["mois"] = date.split("-")[0]+"-"+date.split("-")[1]
    df = df.loc[~ df["code oci"].isnull(),:]
    df = df.drop_duplicates(["code oci", "mois"], keep="first")
    df["code oci id"] = df["code oci"].str.replace("OCI","").astype("float64")
    # get statut == service
    df = df.loc[df["statut"].str.lower() == "service", objet["columns"]]
    logging.info("save to minio")
    save_minio(endpoint, accesskey, secretkey, objet["bucket"], f'{objet["folder"]}-cleaned', date, df)



def cleaning_esco(endpoint:str, accesskey:str, secretkey:str,  date: str)-> None:
    """
    clean opex esco
    
    """
    client = Minio(
        endpoint,
        access_key= accesskey,
        secret_key= secretkey,
        secure=False)
    objet = [d for d in CONFIG["tables"] if d["name"] == "OPEX_ESCO"][0]
    if not client.bucket_exists(objet["bucket"]):
        raise OSError(f"bucket {objet['bucket']} don\'t exits")
    filename = f"OPEX_ESCO_{date.split('-')[0]}{date.split('-')[1]}.xlsx"
    try:
        logging.info("read %s", filename)
        df = pd.read_excel(f"s3://{objet['bucket']}/{objet['folder']}/{filename}",
            storage_options={
            "key": accesskey,
            "secret": secretkey,
            "client_kwargs": {"endpoint_url": f"http://{endpoint}"}
            }
                )
    except Exception as error:
        raise OSError(f"{filename} don't exists in bucket") from error
        

    # check columns
    logging.info("check columns")
    df.columns = df.columns.str.lower()
    missing_columns = set(objet["columns"]).difference(set(df.columns))
    if len(missing_columns):
        raise ValueError(f"missing columns {', '.join(missing_columns)}")
    logging.info("columns are ok")
    logging.info("clean ans enrich")
    cols_to_trim = ["code site oci", "code site"]
    df[cols_to_trim] = df[cols_to_trim].apply(lambda x: x.astype("str"))
    df[cols_to_trim] = df[cols_to_trim].apply(lambda x: x.str.strip())
    df["mois"] = date.split("-")[0]+"-"+date.split("-")[1]
    df = df.loc[~ df["code oci"].isnull(),:]
    df = df.drop_duplicates(["code site oci", "mois"], keep="first")
    df = df.loc[~ df["total redevances ht"].isnull()]
    logging.info("save to minio")
    save_minio(endpoint, accesskey, secretkey, objet["bucket"], f'{objet["folder"]}-cleaned', date, df)




def cleaning_ihs(endpoint:str, accesskey:str, secretkey:str,  date: str)-> None:
    """
     clean or enrich  ihs
    """
    