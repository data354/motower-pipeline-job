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
        df_[objet["columns"]] = df_[objet["columns"]].apply(lambda x: x.astype("str"))
        df_[cols_to_trim] = df_[cols_to_trim].apply(lambda x: x.str.strip())
        df_["mois"] = date.split("-")[0]+"-"+date.split("-")[1]
        df_ = df_.loc[~ df_["code oci"].isnull(),:]
        df_ = df_.drop_duplicates(["code oci", "mois"], keep="first")
        df_["code oci id"] = df_["code oci"].str.replace("OCI","").astype("float64")
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
            logging.info("save to minio")
            save_minio(endpoint, accesskey, secretkey, objet["bucket"], f'{objet["folder"]}-cleaned', date, pd.concat([data, data1, data2]))



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



################################## joindre les tables

def pareto(df):
  """
    add pareto
  """
  total = df.CA_TOTAL.sum()
  df = df.sort_values(by="CA_TOTAL", ascending = False)
  df["sommecum"] = df.CA_TOTAL.cumsum()
  df.loc[df.sommecum<total*0.8 ,"PARETO"] = 1
  df.loc[df.sommecum>total*0.8 ,"PARETO"] = 0
  return df.drop(columns = ["sommecum"])


def oneforall(endpoint:str, accesskey:str, secretkey:str,  date: str):
    """
       merge all data and generate oneforall
    """
    #get bdd site
    client = Minio(
            endpoint,
            access_key= accesskey,
            secret_key= secretkey,
            secure=False)
    objet = [d for d in CONFIG["tables"] if d["name"] == "BASE_SITES"][0]
    filename = getfilename(endpoint, accesskey, secretkey, objet["bucket"], prefix = f"{objet['folder']}-cleaned/{date.split('-')[0]}/{date.split('-')[1]}")
    try:
        logging.info("read %s", filename)
        bdd = pd.read_excel(f"s3://{objet['bucket']}/{filename}",
                storage_options={
                "key": accesskey,
                "secret": secretkey,
                "client_kwargs": {"endpoint_url": f"http://{endpoint}"}
                }
                    )
    except Exception as error:
        raise OSError(f"{filename} don't exists in bucket") from error
    
    #get CA
    objet = [d for d in CONFIG["tables"] if d["name"] == "ca&parc"][0]
    
        
    filename = getfilename(endpoint, accesskey, secretkey, objet["bucket"], prefix = f"{objet['folder']}-cleaned/{date.split('-')[0]}/{date.split('-')[1]}")
       
    try:        
        caparc = pd.read_csv(f"s3://{objet['bucket']}/{filename}",
                storage_options={
                            "key": accesskey,
                            "secret": secretkey,
                            "client_kwargs": {"endpoint_url": f"http://{endpoint}"}
                                    }
                            )
    except Exception as error:
        raise OSError(f"{filename} don't exists in bucket") from error
    
    # get opex esco

    objet = [d for d in CONFIG["tables"] if d["name"] == "OPEX_ESCO"][0]
        
        
    filename = getfilename(endpoint, accesskey, secretkey, objet["bucket"], prefix = f"{objet['folder']}-cleaned/{date.split('-')[0]}/{date.split('-')[1]}")

    try:
        logging.info("read %s", filename)
        esco = pd.read_excel(f"s3://{objet['bucket']}/{filename}",
                                header = 3, sheet_name="Fichier_de_calcul",
                                storage_options={
                                "key": accesskey,
                                "secret": secretkey,
                "client_kwargs": {"endpoint_url": f"http://{endpoint}"}
                }
                    )
    except Exception as error:
        raise OSError(f"{filename} don't exists in bucket") from error

    # get opex ihs
    objet = [d for d in CONFIG["tables"] if d["name"] == "OPEX_IHS"][0]
    filename = getfilename(endpoint, accesskey, secretkey, objet["bucket"], prefix = f"{objet['folder']}-cleaned/{date.split('-')[0]}/{date.split('-')[1]}")

    try:
        logging.info("read %s", filename)
        ihs = pd.read_excel(f"s3://{objet['bucket']}/{filename}",
                                header = 3, sheet_name="Fichier_de_calcul",
                                storage_options={
                                "key": accesskey,
                                "secret": secretkey,
                "client_kwargs": {"endpoint_url": f"http://{endpoint}"}
                }
                    )
    except Exception as error:
        raise OSError(f"{filename} don't exists in bucket") from error
    
    # get opex indisponibilité

    objet = [d for d in CONFIG["tables"] if d["name"] == "faitalarme"][0]
    filename = getfilename(endpoint, accesskey, secretkey, objet["bucket"], prefix = f"{objet['folder']}-cleaned/{date.split('-')[0]}/{date.split('-')[1]}")

    try:
        logging.info("read %s", filename)
        indisponibilite = pd.read_excel(f"s3://{objet['bucket']}/{filename}",
                                header = 3, sheet_name="Fichier_de_calcul",
                                storage_options={
                                "key": accesskey,
                                "secret": secretkey,
                "client_kwargs": {"endpoint_url": f"http://{endpoint}"}
                }
                    )
    except Exception as error:
        raise OSError(f"{filename} don't exists in bucket") from error

    # get opex indisponibilité

    objet = [d for d in CONFIG["tables"] if d["name"] == "hourly_datas_radio_prod"][0]
    filename = getfilename(endpoint, accesskey, secretkey, objet["bucket"], prefix = f"{objet['folder']}-cleaned/{date.split('-')[0]}/{date.split('-')[1]}")

    try:
        logging.info("read %s", filename)
        trafic = pd.read_excel(f"s3://{objet['bucket']}/{filename}",
                                header = 3, sheet_name="Fichier_de_calcul",
                                storage_options={
                                "key": accesskey,
                                "secret": secretkey,
                "client_kwargs": {"endpoint_url": f"http://{endpoint}"}
                }
                    )
    except Exception as error:
        raise OSError(f"{filename} don't exists in bucket") from error

    logging.info("merge bdd and CA")
    bdd_CA = bdd.merge(caparc, left_on=["mois", "code oci id"], right_on = ["month_id", "id_site" ], how="left")
    logging.info("add opex")
    ihs = ihs.loc[:,["month","site id ihs","month_total"]]
    bdd_CA_ihs = bdd_CA.merge(ihs, left_on=["mois", "autre code"], right_on=["month", "site id ihs"], how="left")
    esco = esco.loc[:,["mois","code site", "tital redevances ht"]]
    bdd_CA_ihs_esco = bdd_CA_ihs.merge(esco, left_on=["mois", "autre code"], right_on=["mois","code site"], how="left")
    bdd_CA_ihs_esco.loc[bdd_CA_ihs_esco["total redevances ht"].notnull(), "month_total"] = bdd_CA_ihs_esco["total redevances ht"]

    logging.info("add indisponibilite")
    bdd_CA_ihs_esco_ind = bdd_CA_ihs_esco.merge(indisponibilite, left_on =["code oci", "mois"], right_on = ["code_site","mois"], how="left" )

    logging.info("add trafic")
    bdd_CA_ihs_esco_ind_trafic = bdd_CA_ihs_esco_ind.merge(trafic, left_on =["code oci", "mois"], right_on = ["code_site","mois"], how="left" )

    df_final = bdd_CA_ihs_esco_ind_trafic.loc[:,[ 'mois','code oci','site','autre code','longitude', 'latitude', 'type du site',
       'statut','localisation', 'commune', 'departement', 'region', 'partenaires','proprietaire', 'gestionnaire','type geolocalite', 'projet',
        'position site', 'ca_voix', 'ca_data', 'parc_global', 'parc data',"month_total",'delay_2G', 'delay_3G', 'delay_4G','nbrecellule_2G', 'nbrecellule_3G', 'nbrecellule_4G'
        ,"trafic_voix_2G",	"trafic_voix_3G",	"trafic_voix_4G",	"trafic_data_2G",	"trafic_data_3G",	"trafic_data_4G"
       ]]
    df_final.columns = ['MOIS', 'CODE OCI','SITE', 'AUTRE CODE', 'LONGITUDE', 'LATITUDE',
       'TYPE DU SITE', 'STATUT', 'LOCALISATION', 'COMMUNE', 'DEPARTEMENT', 'REGION',
       'PARTENAIRES', 'PROPRIETAIRE', 'GESTIONNAIRE', 'TYPE GEOLOCALITE',
       'PROJET', 'POSITION SITE', 'CA_VOIX', 'CA_DATA', 'PARC_GLOBAL',
       'PARC DATA', 'OPEX',  'delay_2G', 'delay_3G', 'delay_4G', 'nbrecellule_2G', 'nbrecellule_3G', 'nbrecellule_4G', "trafic_voix_2G",	"trafic_voix_3G",	"trafic_voix_4G",	"trafic_data_2G",	"trafic_data_3G",	"trafic_data_4G"]
    
    df_final["CA_TOTAL"] = df_final["CA_DATA"] + df_final["CA_VOIX"]
    df_final.loc[((df_final.LOCALISATION.str.lower()=="abidjan") & (df_final.CA_TOTAL>=20000000)) | ((df_final.LOCALISATION.str.lower()=="intérieur") & (df_final.CA_TOTAL>=10000000)),["SEGMENT"]] = "PREMIUM"
    df_final.loc[((df_final.LOCALISATION.str.lower()=="abidjan") & ((df_final.CA_TOTAL>=10000000) & (df_final.CA_TOTAL<20000000) )) | ((df_final.LOCALISATION.str.lower()=="intérieur") & ((df_final.CA_TOTAL>=4000000) & (df_final.CA_TOTAL<10000000))),["SEGMENT"]] = "NORMAL"
    df_final.loc[((df_final.LOCALISATION.str.lower()=="abidjan") & (df_final.CA_TOTAL<10000000)) | ((df_final.LOCALISATION.str.lower()=="intérieur") & (df_final.CA_TOTAL<4000000)),["SEGMENT"]] = "A DEVELOPPER"


    # pareto
    oneforall = pareto(df_final)


    interco = 0.139 #CA-voix 
    impot = 0.116 #CA
    frais_dist =  0.09 #CA
    seuil_renta = 0.8


    oneforall["INTERCO"] = oneforall["CA_VOIX"]*interco
    oneforall["IMPOT"] = oneforall["CA_TOTAL"]*impot
    oneforall["FRAIS_DIST"] = oneforall["CA_TOTAL"]*frais_dist
    oneforall["OPEX TOTAL"] = oneforall['OPEX_ITN'] + oneforall["INTERCO"] + oneforall["IMPOT"] + oneforall["FRAIS_DIST"]

    oneforall["EBITDA"] = oneforall["CA_TOTAL"] - oneforall["OPEX"]

    oneforall["RENTABLE"] = (oneforall["EBITDA"]/oneforall["OPEX"])>seuil_renta
    oneforall["NIVEAU_RENTABILITE"] = "NEGATIF"
    oneforall.loc[(oneforall["EBITDA"]/oneforall["OPEX"])>0, "NIVEAU_RENTABILITE"] = "0-80%"
    oneforall.loc[(oneforall["EBITDA"]/oneforall["OPEX"])>0.8, "NIVEAU_RENTABILITE"] = "+80%"
    return oneforall
