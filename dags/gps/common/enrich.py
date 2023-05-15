""" enrich data"""

import pandas as pd
import calendar
from datetime import datetime, timedelta
from minio import Minio
from copy import deepcopy
from gps import CONFIG
from gps.common.rwminio import getfilename
import logging

################################## joindre les tables
def get_number_days(mois: str):
  return calendar.monthrange(int(mois.split("-")[0]), int(mois.split("-")[1]))[1]

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

def prev_segment(df):
    
    past_site = None
    past_segment = None
    for idx, row in df.iterrows():
        if past_site == row["CODE OCI"]:
            df.loc[idx, "PREVIOUS_SEGMENT"] = past_segment
            past_segment = row["SEGMENT"]
        else: 
            past_site = row["CODE OCI"]
            past_segment = row["SEGMENT"]
            df.loc[idx,"PREVIOUS_SEGMENT"] = None
    return df

def oneforall(endpoint:str, accesskey:str, secretkey:str,  date: str, start_date):
    """
       merge all data and generate oneforall
    """
    #get bdd site
    objet = [d for d in CONFIG["tables"] if d["name"] == "BASE_SITES"][0]
    filename = getfilename(endpoint, accesskey, secretkey, objet["bucket"], prefix = f"{objet['folder']}-cleaned/{date.split('-')[0]}/{date.split('-')[1]}")
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
                                
                                storage_options={
                                "key": accesskey,
                                "secret": secretkey,
                "client_kwargs": {"endpoint_url": f"http://{endpoint}"}
                }
                    )
    except Exception as error:
        raise OSError(f"{filename} don't exists in bucket") from error

    # get opex ihs
    if date.split('-')[1] in ["01","02", "03"]:
        pre = "01"
    elif date.split('-')[1] in ["04","05", "06"]:
        pre = "04"
    elif date.split('-')[1] in ["07","08", "09"]:
        pre = "07"
    else:
        pre = "10"
    
    objet = [d for d in CONFIG["tables"] if d["name"] == "OPEX_IHS"][0]
    filename = getfilename(endpoint, accesskey, secretkey, objet["bucket"], prefix = f"{objet['folder']}-cleaned/{date.split('-')[0]}/{pre}")

    try:
        logging.info("read %s", filename)
        ihs = pd.read_csv(f"s3://{objet['bucket']}/{filename}",
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
                                storage_options={
                                "key": accesskey,
                                "secret": secretkey,
                "client_kwargs": {"endpoint_url": f"http://{endpoint}"}
                }
                    )
    except Exception as error:
        raise OSError(f"{filename} don't exists in bucket") from error
    
    # get CSSR
    objet = [d for d in CONFIG["tables"] if "Taux_succes_2" in d["name"] ][0]
    filename = getfilename(endpoint, accesskey, secretkey, objet["bucket"], prefix = f"{objet['bucket']}-cleaned/{date.split('-')[0]}/{date.split('-')[1]}")
    try:
        logging.info("read %s", filename)
        cssr = pd.read_csv(f"s3://{objet['bucket']}/{filename}",
                                storage_options={
                                "key": accesskey,
                                "secret": secretkey,
                "client_kwargs": {"endpoint_url": f"http://{endpoint}"}
                }
                    )
    except Exception as error:
        raise OSError(f"{filename} don't exists in bucket") from error

    logging.info("merge bdd and CA")
    bdd_CA = bdd.merge(caparc, left_on=["code oci id"], right_on = ["id_site" ], how="left")
    print(bdd_CA.columns)
    logging.info("add opex")
    
    bdd_CA_ihs = bdd_CA.merge(ihs, left_on=[ "autre code", "mois"], right_on=[ "site id ihs", "mois"], how="left")
    bdd_CA_ihs_esco = bdd_CA_ihs.merge(esco, left_on=["autre code"], right_on=["code site"], how="left")
    print(bdd_CA_ihs_esco.columns)
    

    # join esco and ihs colonnes
    
    bdd_CA_ihs_esco.loc[bdd_CA_ihs_esco["total redevances ht"].notnull(), "month_total"] = bdd_CA_ihs_esco["total redevances ht"]

    bdd_CA_ihs_esco.loc[bdd_CA_ihs_esco["o&m_x"].isnull(), "o&m_x"] = bdd_CA_ihs_esco["o&m_y"]
    bdd_CA_ihs_esco.loc[bdd_CA_ihs_esco["energy_x"].isnull(), "energy_x"] = bdd_CA_ihs_esco["energy_y"]
    bdd_CA_ihs_esco.loc[bdd_CA_ihs_esco["infra_x"].isnull(), "infra_x"] = bdd_CA_ihs_esco["infra_y"]
    bdd_CA_ihs_esco.loc[bdd_CA_ihs_esco["maintenance passive préventive_x"].isnull(), "maintenance passive préventive_x"] = bdd_CA_ihs_esco["maintenance passive préventive_y"]
    bdd_CA_ihs_esco.loc[bdd_CA_ihs_esco["gardes de sécurité_x"].isnull(), "gardes de sécurité_x"] = bdd_CA_ihs_esco["gardes de sécurité_y"]
    bdd_CA_ihs_esco.loc[bdd_CA_ihs_esco["discount_x"].isnull(), "discount_x"] = bdd_CA_ihs_esco["discount_y"]
    bdd_CA_ihs_esco.loc[bdd_CA_ihs_esco["volume discount_x"].isnull(), "volume discount_x"] = bdd_CA_ihs_esco["volume discount_y"]
    
    logging.info("add indisponibilite")
    bdd_CA_ihs_esco_ind = bdd_CA_ihs_esco.merge(indisponibilite, left_on =["code oci"], right_on = ["code_site"], how="left" )

    logging.info("add trafic")
    bdd_CA_ihs_esco_ind_trafic = bdd_CA_ihs_esco_ind.merge(trafic, left_on =["code oci"], right_on = ["code_site"], how="left" )
    logging.info("add cssr")
    bdd_CA_ihs_esco_ind_trafic_cssr = bdd_CA_ihs_esco_ind_trafic.merge(cssr, left_on =["code oci"], right_on = ["code_site"], how="left" )
    print(bdd_CA_ihs_esco_ind_trafic_cssr.columns)

    df_final = bdd_CA_ihs_esco_ind_trafic_cssr.loc[:,[ 'mois_x','code oci','site','autre code','longitude', 'latitude',
                                                       'type du site', 'statut','localisation', 'commune', 'departement', 'region',
                                                         'partenaires','proprietaire', 'gestionnaire','type geolocalite',
                                                           'projet', 'position site', 'ca_voix', 'ca_data', 'parc_voix', 'parc_data',
                                                           'o&m_x', 'energy_x', 'infra_x', 'maintenance passive préventive_x',
       'gardes de sécurité_x', 'discount_x', 'volume discount_x' ,'tva : 18%', "month_total",'delay_2G', 'delay_3G', 'delay_4G','nbrecellule_2G', 'nbrecellule_3G', 'nbrecellule_4G'
        ,"trafic_voix_2G",	"trafic_voix_3G",	"trafic_voix_4G",	"trafic_data_2G",	"trafic_data_3G",	"trafic_data_4G","avg_cssr_cs_2G"	,"avg_cssr_cs_3G"]]
    
    df_final.columns = ['MOIS', 'CODE OCI','SITE', 'AUTRE CODE', 'LONGITUDE', 'LATITUDE',
       'TYPE DU SITE', 'STATUT', 'LOCALISATION', 'COMMUNE', 'DEPARTEMENT', 'REGION',
       'PARTENAIRES', 'PROPRIETAIRE', 'GESTIONNAIRE', 'TYPE GEOLOCALITE',
       'PROJET', 'POSITION SITE', 'CA_VOIX', 'CA_DATA', 'PARC_GLOBAL',
       'PARC DATA', 'O&M', 'Energy', 'Infra', 'Maintenance Passive préventive',
       'Gardes de sécurité', 'Discount', 'Volume discount' ,'TVA : 18%', 'OPEX',  'delay_2G', 'delay_3G', 'delay_4G', 'nbrecellule_2G', 'nbrecellule_3G', 'nbrecellule_4G', 'Cellules_2G_congestionnées', 'Cellules_2G',
       'Cellules_3G_congestionnées', 'Cellules_3G',
       'Cellules_4G_congestionnées', 'Cellules_4G', "trafic_voix_2G",	"trafic_voix_3G",	"trafic_voix_4G",	"trafic_data_2G",	"trafic_data_3G",	"trafic_data_4G","avg_cssr_cs_2G"	,"avg_cssr_cs_3G"]
    
    df_final["trafic_voix_total"] = df_final["trafic_voix_2G"]+df_final["trafic_voix_3G"] + df_final["trafic_voix_4G"]
    df_final["trafic_data_total"] = df_final["trafic_data_2G"]+df_final["trafic_data_3G"] + df_final["trafic_data_4G"]

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
    oneforall["OPEX TOTAL"] = oneforall['OPEX'] + oneforall["INTERCO"] + oneforall["IMPOT"] + oneforall["FRAIS_DIST"]

    oneforall["EBITDA"] = oneforall["CA_TOTAL"] - oneforall["OPEX"]

    oneforall["RENTABLE"] = (oneforall["EBITDA"]/oneforall["OPEX"])>seuil_renta
    oneforall["NIVEAU_RENTABILITE"] = "NEGATIF"
    oneforall.loc[(oneforall["EBITDA"]/oneforall["OPEX"])>0, "NIVEAU_RENTABILITE"] = "0-80%"
    oneforall.loc[(oneforall["EBITDA"]/oneforall["OPEX"])>0.8, "NIVEAU_RENTABILITE"] = "+80%"

    logging.info("add NUR") # a modifier
    cellule_total_2G = 13485.0
    cellule_total_3G = 26156.0
    cellule_total_4G = 17862.0
    print(oneforall.loc[oneforall.MOIS.isnull(),:])
    oneforall["days"] = oneforall["MOIS"].apply(get_number_days)

    oneforall["NUR_2G"] = (100000 * oneforall['nbrecellule_2G'] * oneforall['delay_2G'] )/ (3600*24*oneforall["days"]  * cellule_total_2G )
    oneforall["NUR_3G"] = (100000 * oneforall['nbrecellule_3G'] * oneforall['delay_3G'] )/ (3600*24*oneforall["days"]  * cellule_total_3G )
    oneforall["NUR_4G"] = (100000 * oneforall['nbrecellule_4G'] * oneforall['delay_4G'] )/ (3600*24*oneforall["days"]  * cellule_total_4G )

    oneforall["PREVIOUS_SEGMENT"] = None
    if datetime.strptime(date, "%Y-%m-%d") > datetime.strptime(start_date, "%Y-%m-%d"):
        last = datetime.strptime(date, "%Y-%m-%d") - timedelta(weeks=4)
        last_filename = getfilename(endpoint, accesskey, secretkey, "oneforall", prefix = f"{last.year}/{str(last.month).zfill(2)}")
        lastoneforall = pd.read_csv(f"s3://oneforall/{last_filename}",
                                storage_options={
                                "key": accesskey,
                                "secret": secretkey,
                "client_kwargs": {"endpoint_url": f"http://{endpoint}"}
                }
                    )

        big = pd.concat([lastoneforall, oneforall])
        big = big.sort_values(["CODE OCI", "MOIS"])
        final = prev_segment(big)
        final = final.loc[final.MOIS == oneforall.MOIS.unique(), :]
        return final
    return oneforall