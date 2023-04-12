from pathlib import Path
import pandas as pd
from minio import Minio
from copy import deepcopy
from pandas.io.excel import ExcelFile

from gps import CONFIG
from gps.common.rwminio import save_minio, getfilename, getfilesnames
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
    logging.info("get filename")

    filename = getfilename(endpoint, accesskey, secretkey, objet["bucket"], prefix = f"{objet['folder']}/{objet['folder']}_{date.split('-')[0]}{date.split('-')[1]}")
    try:
        logging.info("read %s", filename)
        df_ = pd.read_excel(f"s3://{objet['bucket']}/{filename}",
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
    df_.columns = df_.columns.str.lower()
    missing_columns = set(objet["columns"]).difference(set(df_.columns))
    if len(missing_columns):
        raise ValueError(f"missing columns {', '.join(missing_columns)}")
    logging.info("columns are ok")
   
    # strip columns
    logging.info("clean and enrich")
    cols_to_trim = ["code oci", "autre code"]
    df_[objet["columns"]] = df_[objet["columns"]].apply(lambda x: x.astype("str"))
    df_[cols_to_trim] = df_[cols_to_trim].apply(lambda x: x.str.strip())
    df_["mois"] = date.split("-")[0]+"-"+date.split("-")[1]
    df_ = df_.loc[~ df_["code oci"].isnull(),:]
    df_ = df_.drop_duplicates(["code oci", "mois"], keep="first")
    df_["code oci id"] = df_["code oci"].str.replace("OCI","").astype("float64")
    # get statut == service
    df_ = df_.loc[df_["statut"].str.lower() == "service", objet["columns"]]
    df_ = df_.loc[(df_["position site"].str.lower() == "localité") & (df_["position site"].str.lower() == "localié"), objet["columns"]]
    logging.info("save to minio")
    save_minio(endpoint, accesskey, secretkey, objet["bucket"], f'{objet["folder"]}-cleaned', date, df_)



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
    
    filename = getfilename(endpoint, accesskey, secretkey, objet["bucket"], prefix = f"{objet['folder']}/{objet['folder']}_{date.split('-')[0]}{date.split('-')[1]}")

    try:
        logging.info("read %s", filename)
        df_ = pd.read_excel(f"s3://{objet['bucket']}/{filename}",
                            header = 3, sheet_name="Fichier_de_calcul",
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
    df_.columns = df_.columns.str.lower()
    missing_columns = set(objet["columns"]).difference(set(df_.columns))
    if len(missing_columns):
        raise ValueError(f"missing columns {', '.join(missing_columns)}")
    logging.info("columns are ok")
    logging.info("clean ans enrich")
    cols_to_trim = ["code site oci", "code site"]
    df_[cols_to_trim] = df_[cols_to_trim].apply(lambda x: x.astype("str"))
    df_[cols_to_trim] = df_[cols_to_trim].apply(lambda x: x.str.strip())
    df_["mois"] = date.split("-")[0]+"-"+date.split("-")[1]
    df_ = df_.loc[~ df_["code site oci"].isnull(),:]
    df_ = df_.drop_duplicates(["code site oci", "mois"], keep="first")
    df_ = df_.loc[~ df_["total redevances ht"].isnull()]
    logging.info("save to minio")
    save_minio(endpoint, accesskey, secretkey, objet["bucket"], f'{objet["folder"]}-cleaned', date, df_)




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

        logging.info("get filename")
        filename = getfilename(endpoint, accesskey, secretkey, objet["bucket"], prefix = f"{objet['folder']}/{objet['folder']}_{date.split('-')[0]}{date.split('-')[1]}")
        logging.info("read file %s",filename)
        
        
       
        excel = pd.ExcelFile(f"s3://{objet['bucket']}/{filename}",
                             storage_options={
                            "key": accesskey,
                            "secret": secretkey,
                            "client_kwargs": {"endpoint_url": f"http://{endpoint}"}
                                } )
        sheet_f = []
        for sheet in objet["sheets"]:
           sheet_f.extend([s for s in excel.sheet_names if s.find(sheet)!=-1])
        print(len(sheet_f))
        data = pd.DataFrame()
        logging.info("read %s", filename)
        for sh in sheet_f:
            header = 14 if sh.find("OCI-COLOC") != -1 else 15
            try:
                
                df_ = pd.read_excel(f"s3://{objet['bucket']}/{filename}",
                                 header=header , sheet_name=sh,
                    storage_options={
                        "key": accesskey,
                        "secret": secretkey,
                        "client_kwargs": {"endpoint_url": f"http://{endpoint}"}
                                }
                        )
                df_.columns = df_.columns.str.lower()
                columns_to_check = ['site id ihs', 'site name', 'category', 'trimestre ht'] if sh.find("OCI-MLL BPCI 22") == -1 else objet["columns"]
                missing_columns = set(columns_to_check).difference(set(df_.columns))
                if len(missing_columns):
                    raise ValueError(f"missing columns {', '.join(missing_columns)} in sheet {sh} of file {filename}")
                df_ = df_.loc[:, ['site id ihs', 'site name', 'category', 'trimestre ht']] if sh.find("OCI-MLL BPCI 22") == -1 else df_.loc[:, objet['columns']]
                df_["month_total"] = df_['trimestre ht'] / 3 if sh.find("OCI-MLL BPCI 22") == -1 else df_['trimestre 1 - ht'] / 3
                data = pd.concat([data, df_])
            except Exception as error:
                raise OSError(f"{filename} don't exists in bucket") from error


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



########## enrich

def cleaning_ca_parc(endpoint:str, accesskey:str, secretkey:str,  date: str):
    """
     cleaning CA & Parc
    """

    client = Minio(
        endpoint,
        access_key= accesskey,
        secret_key= secretkey,
        secure=False)
    objet = [d for d in CONFIG["tables"] if d["name"] == "ca&parc"][0]
    if not client.bucket_exists(objet["bucket"]):
            raise OSError(f"bucket {objet['bucket']} don\'t exits")
    
    filenames = getfilesnames(endpoint, accesskey, secretkey, objet["bucket"], prefix = f"{objet['folder']}/{date.split('-')[0]}/{date.split('-')[1]}")
    data = pd.DataFrame()
    for filename in filenames:
        try:
                
                df_ = pd.read_csv(f"s3://{objet['bucket']}/{filename}",
                    storage_options={
                        "key": accesskey,
                        "secret": secretkey,
                        "client_kwargs": {"endpoint_url": f"http://{endpoint}"}
                                }
                        )
        except Exception as error:
                raise OSError(f"{filename} don't exists in bucket") from error
        
        data = pd.concat([data, df_])
    
    logging.info("check columns")
    data.columns = data.columns.str.lower()
    missing_columns = set(objet["columns"]).difference(set(data.columns))
    if len(missing_columns):
        raise ValueError(f"missing columns {', '.join(missing_columns)}")
    logging.info("columns are ok")
    logging.info("clean ans enrich")
    data["id_site"] = data['id_site'].astype("str")
    data["mois"] = date.split("-")[0]+"-"+date.split("-")[1]
    data = data.loc[~ data["code site oci"].isnull(),:]
    data = data.drop_duplicates(["id_site", "mois"], keep="first").loc[:,["mois",	"id_site",	"ca_voix",	"ca_data"]]
    logging.info("save to minio")
    save_minio(endpoint, accesskey, secretkey, objet["bucket"], f'{objet["folder"]}-cleaned', date, data)
