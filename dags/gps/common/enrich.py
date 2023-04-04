from pathlib import Path
import pandas as pd
from minio import Minio
from copy import deepcopy
from pandas.io.excel import ExcelFile

from gps import CONFIG
from gps.common.rwminio import save_minio, getfilename
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
    #filename = f"BASE_SITES_{date.split('-')[0]}{date.split('-')[1]}.xlsx"
    
    logging.info("get filename")

    filename = getfilename(endpoint, accesskey, secretkey, objet["bucket"], objet["folder"],date)
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
    #filename = f"OPEX_ESCO_{date.split('-')[0]}{date.split('-')[1]}.xlsx"
    logging.info("get filename")

    filename = getfilename(endpoint, accesskey, secretkey, objet["bucket"], objet["folder"],date)
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
    
    if date.split("-")[1] in ["01","04","07","10"]:
        
        client = Minio(
        endpoint,
        access_key= accesskey,
        secret_key= secretkey,
        secure=False)
        objet = [d for d in CONFIG["tables"] if d["name"] == "OPEX_IHS"][0]
        
        if not client.bucket_exists(objet["bucket"]):
            raise OSError(f"bucket {objet['bucket']} don\'t exits")

        #filename = f"OPEX_IHS_{date.split('-')[0]}{date.split('-')[1]}.xlsx"
        logging.info("get filename")
        filename = getfilename(endpoint, accesskey, secretkey, objet["bucket"], objet["folder"],date)
        logging.info("read file %s",filename)
        excel = pd.ExcelFile(filename)
        sheet_f = []
        for sheet in objet["sheets"]:
            sheet_f.extend([s for s in excel.sheet_names if s.find(sheet)!=-1])

        data = pd.DataFrame()
        for s in sheet_f:
            header = 14 if s.find("OCI-COLOC") != -1 else 15
            try:
                logging.info("read %s", filename)
                df = pd.read_excel(f"s3://{objet['bucket']}/{objet['folder']}/{filename}", header=header ,
                    storage_options={
                        "key": accesskey,
                        "secret": secretkey,
                        "client_kwargs": {"endpoint_url": f"http://{endpoint}"}
                                }
                        )
                df.columns = df.columns.str.lower()
            except Exception as error:
                raise OSError(f"{filename} don't exists in bucket") from error
            

            missing_columns = set(objet["columns"]).difference(set(df.columns))
            if len(missing_columns):
                raise ValueError(f"missing columns {', '.join(missing_columns)}")
            
            
            df = df.loc[:, ['site id ihs', 'site name', 'category', 'trimestre ht']] if s.find("OCI-MLL BPCI 22") == -1 else df.loc[:, objet['columns']]
            df["month_total"] = df['trimestre ht'] / 3 if s.find("OCI-MLL BPCI 22") == -1 else df['trimestre 1 - ht'] / 3
            
            data = pd.concat([data, df])


        logging.info("clean ans enrich")

        cols_to_trim = ['site id ihs']
        data[cols_to_trim] = data[cols_to_trim].apply(lambda x: x.astype("str"))
        data[cols_to_trim] = data[cols_to_trim].apply(lambda x: x.str.strip())
        data["month"] = date.split("-")[0]+"-"+date.split("-")[1]
        data = data.loc[~ data['site id ihs'].isnull(),:]
        data.loc[data["trimestre ht"].isna(),"trimestre ht"] = data.loc[data["trimestre 1 - ht"].notna(), "trimestre 1 - ht"]
        data = data.loc[~ data["trimestre ht"].isnull(),:]
        data1 = deepcopy(data)
        data1["mois"] = date.split("-")[0]+"-"+str(int(date.split("-")[1])+1).zfill(2)
        data2 = deepcopy(data)
        data2["mois"] = date.split("-")[0]+"-"+str(int(date.split("-")[1])+2).zfill(2)
        logging.info("save to minio")
        save_minio(endpoint, accesskey, secretkey, objet["bucket"], f'{objet["folder"]}-cleaned', date, pd.concat([data, data1, data2]))


