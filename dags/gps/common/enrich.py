""" enrich data"""

import pandas as pd
from datetime import datetime
from minio import Minio
from copy import deepcopy
from gps import CONFIG
from gps.common.rwminio import getfilename
import logging

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
    objet = [d for d in CONFIG["tables"] if d["name"] == "BASE_SITES"][0]
    filename = getfilename(endpoint, accesskey, secretkey, objet["bucket"], prefix = f"{objet["folder"]}-cleaned/{date.split('-')[0]}/{date.split('-')[1]}")
    try:
        logging.info("read %s", filename)
        bdd = pd.read_csv(f"s3://{objet['bucket']}/{filename}",
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
        esco = pd.read_csv(f"s3://{objet['bucket']}/{filename}",
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
        ihs = pd.read_csv(f"s3://{objet['bucket']}/{filename}",
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
        indisponibilite = pd.read_csv(f"s3://{objet['bucket']}/{filename}",
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
        trafic = pd.read_csv(f"s3://{objet['bucket']}/{filename}",
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
