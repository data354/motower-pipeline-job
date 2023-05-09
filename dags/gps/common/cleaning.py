from pathlib import Path
import pandas as pd
from datetime import datetime
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
    if datetime.strptime(date, "%Y-%m-%d") >= datetime(2022,11,6):
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
        objet["columns"].append("mois")
        df_["mois"] = date.split("-")[0]+"-"+date.split("-")[1]
        df_[objet["columns"]] = df_[objet["columns"]].apply(lambda x: x.astype("str"))
        df_[cols_to_trim] = df_[cols_to_trim].apply(lambda x: x.str.strip())
        df_ = df_.loc[~ df_["code oci"].isnull(),:]
        df_ = df_.drop_duplicates(["code oci", "mois"], keep="first")
        df_["code oci id"] = df_["code oci"].str.replace("OCI","").astype("float64")
        objet["columns"].append("code oci id")
        # get statut == service
        df_ = df_.loc[df_["statut"].str.lower() == "service", objet["columns"]]
        df_ = df_.loc[(df_["position site"].str.lower() == "localité") | (df_["position site"].str.lower() == "localié"), objet["columns"]]
        logging.info("save to minio")
        save_minio(endpoint, accesskey, secretkey, objet["bucket"], f'{objet["folder"]}-cleaned', date, df_)



def cleaning_esco(endpoint:str, accesskey:str, secretkey:str,  date: str)-> None:
    """
    clean opex esco
    
    """
    if datetime.strptime(date, "%Y-%m-%d") >= datetime(2022,10,6):
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
        if "volume discount" not in df_.columns:
            df_["volume discount"] = 0
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
    if datetime.strptime(date, "%Y-%m-%d") >= datetime(2022,7,6):
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
            data["mois"] = date.split("-")[0]+"-"+date.split("-")[1]
            data = data.loc[~ data['site id ihs'].isnull(),:]
            data = data.drop_duplicates(["site id ihs", "mois"], keep="first")
            
            data.loc[data["trimestre ht"].isna(),"trimestre ht"] = data.loc[data["trimestre 1 - ht"].notna(), "trimestre 1 - ht"]
            data = data.loc[~ data["trimestre ht"].isnull(),:]
            data1 = deepcopy(data)
            data1["mois"] = date.split("-")[0]+"-"+str(int(date.split("-")[1])+1).zfill(2)
            data2 = deepcopy(data)
            data2["mois"] = date.split("-")[0]+"-"+str(int(date.split("-")[1])+2).zfill(2)
            data_final = pd.concat([data, data1, data2])

            if datetime.strptime(date, "%Y-%m-%d") >= datetime(2022,10,6):
                logging.info("Add breakout data")

                #download esco to make ratio
                esco_objet = [d for d in CONFIG["tables"] if d["name"] == "OPEX_ESCO"][0]
                filename = getfilename(endpoint, accesskey, secretkey, esco_objet["bucket"], prefix = f"{esco_objet['folder']}-cleaned/{date.split('-')[0]}/{date.split('-')[1]}")
                try:
                    logging.info("read %s", filename)
                    esco = pd.read_csv(f"s3://{esco_objet['bucket']}/{filename}",
                                    
                                    storage_options={
                                    "key": accesskey,
                                    "secret": secretkey,
                    "client_kwargs": {"endpoint_url": f"http://{endpoint}"}
                    }
                        )
                except Exception as error:
                    raise OSError(f"{filename} don't exists in bucket") from error
                
                
                
                esco["opex_without_discount"] = esco["total redevances ht"] + esco["discount"] + esco["volume discount"]
                esco = esco.groupby("mois").sum()
                esco = esco.loc[:,["o&m", 'energy',	"infra"	, "maintenance passive préventive", "gardes de sécurité","discount", "volume discount", "opex_without_discount"]]
                ratio = esco.loc[: ,["o&m", 'energy',	"infra"	, "maintenance passive préventive", "gardes de sécurité","discount"]].divide(esco["opex_without_discount"].values[0])
                ratio["volume discount"] = esco["volume discount"].divide(esco["opex_without_discount"].values[0])
                print(ratio)

                data_final["Discount"] = 0
                data_final["Volume discount"] = 0    
                
                
                data_final.loc[:, "O&M"] = ratio["O&M"] * data_final.loc[:,"month_total"]
                data_final.loc[:, "Energy"] = ratio["Energy"] * data_final.loc[:,"month_total"]
                data_final.loc[:, "Infra"] = ratio["Infra"] * data_final.loc[:,"month_total"]
                data_final.loc[:, "Maintenance Passive préventive"] = ratio["Maintenance Passive préventive"] * data_final.loc[:,"month_total"]
                data_final.loc[:, "Gardes de sécurité"] = ratio["Gardes de sécurité"] * data_final.loc[:,"month_total"]
            else:
                data_final["Discount"] = 0
                data_final["Volume discount"] = 0  
                data_final["O&M"] = 0
                data_final["Energy"] = 0  
                data_final["Infra"] = 0
                data_final["Maintenance Passive préventive"] = 0
                data_final["Gardes de sécurité"] = 0
            logging.info("save to minio")
            save_minio(endpoint, accesskey, secretkey, objet["bucket"], f'{objet["folder"]}-cleaned', date, data_final)



########## enrich

def cleaning_ca_parc(endpoint:str, accesskey:str, secretkey:str,  date: str):
    """
     cleaning CA & Parc
    """
    if datetime.strptime(date, "%Y-%m-%d") >= datetime(2022,9,6):
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
            df_.columns = df_.columns.str.lower()
            data = pd.concat([data, df_])
            data = data.sort_values("day_id")
            data = data.groupby(["id_site", "month_id"]).aggregate({'ca_voix': 'sum', 'ca_data': 'sum','parc_voix': 'last', 'parc_data': 'last'})
        save_minio(endpoint, accesskey, secretkey, objet["bucket"], f'{objet["folder"]}-cleaned', date, data)



def cleaning_alarm(endpoint:str, accesskey:str, secretkey:str,  date: str):
    """
        clean alarm
    """
    if datetime.strptime(date, "%Y-%m-%d") >= datetime(2022,12,6):
        client = Minio(
            endpoint,
            access_key= accesskey,
            secret_key= secretkey,
            secure=False)
        objet = [d for d in CONFIG["tables"] if d["name"] == "faitalarme"][0]
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
            df_.columns = df_.columns.str.lower()
            data = pd.concat([data, df_])

        
        cols_to_trim = ["code_site"]
        data[cols_to_trim] = data[cols_to_trim].apply(lambda x: x.astype("str"))
        data[cols_to_trim] = data[cols_to_trim].apply(lambda x: x.str.strip())
        data.date = data.date.astype("str")

        data["mois"] = date.split("-")[0]+"-"+date.split("-")[1]

        data = data.sort_values("nbrecellule", ascending=False).drop_duplicates(["occurence", "code_site", "techno"], keep ="first" )
        data = data.loc[:, ["code_site", "delay","mois", "techno", "nbrecellule"]]
        # temps d'indisponibilité par site
        data = data.groupby(["code_site", "mois", "techno"]).sum(["delay", "nbrecellule"])
        #unstack var by techno
        data_final = data.unstack()
        data_final.columns = ["_".join(d) for d in data_final.columns]
        save_minio(endpoint, accesskey, secretkey, objet["bucket"], f'{objet["folder"]}-cleaned', date, data_final)



def cleaning_trafic(endpoint:str, accesskey:str, secretkey:str,  date: str):
    """
    cleaning trafic"""

    if datetime.strptime(date, "%Y-%m-%d") >= datetime(2022,12,6):
        client = Minio( endpoint,
            access_key= accesskey,
            secret_key= secretkey,
            secure=False)
        objet = [d for d in CONFIG["tables"] if d["name"] == "hourly_datas_radio_prod"][0]
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
            df_.columns = df_.columns.str.lower()
            data = pd.concat([data, df_])

        cols_to_trim = ["code_site"]
        data[cols_to_trim] = data[cols_to_trim].apply(lambda x: x.astype("str"))
        data[cols_to_trim] = data[cols_to_trim].apply(lambda x: x.str.strip())
        data.date_jour = data.date_jour.astype("str")
        data["mois"] = data.date_jour.str[:4].str.cat(data.date_jour.str[4:6], "-" )
    
        data = data.groupby(["mois","code_site", "techno"]).sum(["trafic_voix","trafic_data"])
        data = data.unstack()
        data.columns = ["_".join(d) for d in data.columns]
        save_minio(endpoint, accesskey, secretkey, objet["bucket"], f'{objet["folder"]}-cleaned', date, data)


def cleaning_call_drop(endpoint:str, accesskey:str, secretkey:str,  date: str):
    """
     cleaning call drop
    """
    client = Minio( endpoint,
        access_key= accesskey,
        secret_key= secretkey,
        secure=False)
    objet_2g = [d for d in CONFIG["tables"] if "Call_drop_2" in d["name"] ][0]
    objet_3g = [d for d in CONFIG["tables"] if "Call_drop_3" in d["name"] ][0]
    if not client.bucket_exists(objet_2g["bucket"]):
            raise OSError(f"bucket {objet_2g['bucket']} don\'t exits")
    
    filenames = getfilesnames(endpoint, accesskey, secretkey, objet_2g["bucket"], prefix = f"{objet_2g['folder']}/{date.split('-')[0]}/{date.split('-')[1]}")
    filenames.extend(getfilesnames(endpoint, accesskey, secretkey, objet_3g["bucket"], prefix = f"{objet_3g['folder']}/{date.split('-')[0]}/{date.split('-')[1]}"))
    data = pd.DataFrame()
    for filename in filenames:
        try:
                
                df_ = pd.read_csv(f"s3://{objet_2g['bucket']}/{filename}",
                    storage_options={
                        "key": accesskey,
                        "secret": secretkey,
                        "client_kwargs": {"endpoint_url": f"http://{endpoint}"}
                                }
                        )
        except Exception as error:
                raise OSError(f"{filename} don't exists in bucket") from error
        df_.columns = df_.columns.str.lower()
        data = pd.concat([data, df_])
    cols_to_trim = ["code_site"]
    data[cols_to_trim] = data[cols_to_trim].apply(lambda x: x.astype("str"))
    data[cols_to_trim] = data[cols_to_trim].apply(lambda x: x.str.strip())
    data.loc[data["drop_after_tch_assign"].isna(),"drop_after_tch_assign"] = data.loc[data["number_of_call_drop_3g"].notna(), "number_of_call_drop_3g"]
    data.date_jour = data.date_jour.astype("str")
    data["mois"] = data.date_jour.str[:4].str.cat(data.date_jour.str[4:6], "-" )
    data = data.loc[:, ["mois","code_site", "techno", "drop_after_tch_assign"]]
    data = data.groupby(["mois","code_site", "techno"]).sum(["drop_after_tch_assign"])
    
    data = data.unstack()
    data.columns = ["_".join(d) for d in data.columns]
    save_minio(endpoint, accesskey, secretkey, objet_2g["bucket"], f'{objet_2g["bucket"]}-cleaned', date, data)



