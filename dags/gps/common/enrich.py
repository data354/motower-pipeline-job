""" enrich data"""

import pandas as pd
import requests
import calendar
from datetime import datetime, timedelta
from minio import Minio
from copy import deepcopy
from gps import CONFIG
from gps.common.rwminio import  get_latest_file
import logging

################################## joindre les tables
def get_number_days(mois: str):
    """
       get number of days in month
    """
    try:
        year, month = map(int, mois.split("-"))
    except ValueError:
        return "Invalid input format. Please use format 'YYYY-MM'."
    return calendar.monthrange(year, month)[1]

def get_number_cellule(dataframe, cellule: str):
    """
        get number of cellule 
    """
    # Get the sum of values in the specified cell for the given month
    cellule_total = dataframe[cellule].sum()
    return cellule_total



def pareto(dataframe):
  """
    add pareto
  """
  total = dataframe.ca_total.sum()
  dataframe = dataframe.sort_values(by="ca_total", ascending = False)
  dataframe["sommecum"] = dataframe.ca_total.cumsum()
  dataframe.loc[dataframe.sommecum<total*0.8 ,"pareto"] = 1
  dataframe.loc[dataframe.sommecum>total*0.8 ,"pareto"] = 0
  dataframe["pareto"] = dataframe["pareto"].astype(bool)
  return dataframe.drop(columns = ["sommecum"])



def prev_segment(dataframe):
    """
        get previous segment
    """
    
    past_site = None
    past_segment = None
    for idx, row in dataframe.iterrows():
        if past_site == row["CODE_OCI"]:
            dataframe.loc[idx, "PREVIOUS_SEGMENT"] = past_segment
            past_segment = row["SEGMENT"]
        else: 
            past_site = row["CODE_OCI"]
            past_segment = row["SEGMENT"]
            dataframe.loc[idx,"PREVIOUS_SEGMENT"] = None
    return dataframe

def get_thresold(code: str):
    """
     get thresold from api
    """
    objets = requests.get(CONFIG["api_params"], timeout=15).json()
    objet =  next((obj for obj in objets if obj["code"] == code), None)
    if not objet:
        raise ValueError("thresold of type %s not available", code)
    return objet.get("value", CONFIG[code]).replace(",",".")

def oneforall(client, endpoint:str, accesskey:str, secretkey:str,  date: str, start_date):
    """
       merge all data and generate oneforall
    """
    date_parts = date.split("-")
    #get bdd site
    objet = next((table for table in CONFIG["tables"] if table["name"] == "BASE_SITES"), None)
    if objet is None:
        raise ValueError("Table 'BASE_SITES' not found in configuration")
    # Parse the date string into datetime object
    
    filename = get_latest_file(client= client, bucket= objet["bucket"], prefix = f"{objet['folder']}-cleaned/{date_parts[0]}/{date_parts[1]}/{date_parts[2]}")
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
    objet = next((table for table in CONFIG["tables"] if table["name"] == "caparc"), None)
    if objet is None:
        raise ValueError("Table 'caparc' not found in configuration")
        
    filename = get_latest_file(client, objet["bucket"], prefix = f"{objet['folder']}-cleaned/{date_parts[0]}/{date_parts[1]}/{date_parts[2]}")

    try:        
        logging.info("read %s", filename)
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

    objet = next((table for table in CONFIG["tables"] if table["name"] == "OPEX_ESCO"), None) 
    if objet is None:
        raise ValueError("Table 'BASE_SITES' not found in configuration")
    
    filename = get_latest_file(client, objet["bucket"], prefix = f"{objet['folder']}-cleaned/{date_parts[0]}/{date_parts[1]}/{date_parts[2]}")
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
    
    objet = next((table for table in CONFIG["tables"] if table["name"] == "OPEX_IHS"), None)
    if objet is None:
        raise ValueError("Table 'OPEX_IHS' not found in configuration")
    filename = get_latest_file(client, objet["bucket"], prefix = f"{objet['folder']}-cleaned/{date_parts[0]}/{pre}/{date_parts[2]}")

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
    
    # get  indisponibilité
    objet = next((table for table in CONFIG["tables"] if table["name"] == "faitalarme"), None)
    if objet is None:
        raise ValueError("Table 'faitalarme' not found in configuration")
  
    filename = get_latest_file(client, objet["bucket"], prefix = f"{objet['folder']}-cleaned/{date_parts[0]}/{date_parts[1]}/{date_parts[2]}")

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

    # get trafic
    objet = next((table for table in CONFIG["tables"] if table["name"] == "hourly_datas_radio_prod"), None)
    if objet is None:
        raise ValueError("Table 'hourly_datas_radio_prod' not found in configuration")
    filename = get_latest_file(client, objet["bucket"], prefix = f"{objet['folder']}-cleaned/{date_parts[0]}/{date_parts[1]}/{date_parts[2]}")

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
    objet = next((table for table in CONFIG["tables"] if table["name"] == "Taux_succes_2g"), None)
    if objet is None:
        raise ValueError("Table 'Taux_succes_2' not found in configuration")
    filename = get_latest_file(client, objet["bucket"], prefix = f"{objet['folder']}-cleaned/{date_parts[0]}/{date_parts[1]}/{date_parts[2]}")

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

    # get congestion
    objet = next((table for table in CONFIG["tables"] if table["name"] == "CONGESTION"), None)
    if objet is None:
        raise ValueError("Table 'CONGESTION' not found in configuration")
    filename = get_latest_file(client, objet["bucket"], prefix = f"{objet['folder']}-cleaned/{date_parts[0]}/{date_parts[1]}/{date_parts[2]}")
    try:
        logging.info("read %s", filename)
        cong = pd.read_csv(f"s3://{objet['bucket']}/{filename}",
                                storage_options={
                                "key": accesskey,
                                "secret": secretkey,
                "client_kwargs": {"endpoint_url": f"http://{endpoint}"}
                }
                    )
    except Exception as error:
        raise OSError(f"{filename} don't exists in bucket") from error


    # merging

    
    logging.info("merge bdd and CA")
    bdd_ca = bdd.merge(caparc, left_on=["code oci id"], right_on = ["id_site" ], how="left")
    
    logging.info("add opex")
    bdd_ca_ihs = bdd_ca.merge(ihs, left_on=[ "autre code", "mois"], right_on=[ "site id ihs", "mois"], how="left")
    bdd_ca_ihs_esco = bdd_ca_ihs.merge(esco, left_on=["autre code"], right_on=["code site"], how="left")
    
    # join esco and ihs colonnes
    
    bdd_ca_ihs_esco.loc[bdd_ca_ihs_esco["total redevances ht"].notnull(), "month_total"] = bdd_ca_ihs_esco["total redevances ht"]

    bdd_ca_ihs_esco.loc[bdd_ca_ihs_esco["o&m_x"].isnull(), "o&m_x"] = bdd_ca_ihs_esco["o&m_y"]
    bdd_ca_ihs_esco.loc[bdd_ca_ihs_esco["energy_x"].isnull(), "energy_x"] = bdd_ca_ihs_esco["energy_y"]
    bdd_ca_ihs_esco.loc[bdd_ca_ihs_esco["infra_x"].isnull(), "infra_x"] = bdd_ca_ihs_esco["infra_y"]
    bdd_ca_ihs_esco.loc[bdd_ca_ihs_esco["maintenance passive preventive_x"].isnull(), "maintenance passive preventive_x"] = bdd_ca_ihs_esco["maintenance passive preventive_y"]
    bdd_ca_ihs_esco.loc[bdd_ca_ihs_esco["gardes de securite_x"].isnull(), "gardes de securite_x"] = bdd_ca_ihs_esco["gardes de securite_y"]
    bdd_ca_ihs_esco.loc[bdd_ca_ihs_esco["discount_x"].isnull(), "discount_x"] = bdd_ca_ihs_esco["discount_y"]
    bdd_ca_ihs_esco.loc[bdd_ca_ihs_esco["volume discount_x"].isnull(), "volume discount_x"] = bdd_ca_ihs_esco["volume discount_y"]
    
    logging.info("add indisponibilite")
    bdd_ca_ihs_esco_ind = bdd_ca_ihs_esco.merge(indisponibilite, left_on =["code oci"], right_on = ["code_site"], how="left" )

    logging.info("add congestion")
    bdd_ca_ihs_esco_ind_cong = bdd_ca_ihs_esco_ind.merge(cong, left_on =["code oci"], right_on = ["code_site"], how="left" )

    logging.info("add trafic")
    bdd_ca_ihs_esco_ind_cong_trafic = bdd_ca_ihs_esco_ind_cong.merge(trafic, left_on =["code oci"], right_on = ["code_site"], how="left" )

    logging.info("add cssr")
    bdd_ca_ihs_esco_ind_cong_trafic_cssr = bdd_ca_ihs_esco_ind_cong_trafic.merge(cssr, left_on =["code oci"], right_on = ["code_site"], how="left" )

    logging.info("final columns")

    df_final = bdd_ca_ihs_esco_ind_cong_trafic_cssr.loc[:,[ 'mois_x','code oci','site','autre code','longitude', 'latitude',
                                                       'type du site', 'statut','localisation', 'commune', 'departement', 'region',
                                                         'partenaires','proprietaire', 'gestionnaire','type geolocalite',
                                                           'projet', 'clutter', 'position site', 'ca_voix', 'ca_data', 'parc_voix', 'parc_data',
                                                           'o&m_x', 'energy_x', 'infra_x', 'maintenance passive preventive_x',
       'gardes de securite_x', 'discount_x', 'volume discount_x' ,'tva : 18%', "month_total",'delay_2G', 'delay_3G', 'delay_4G', 'delaycellule_2G', 'delaycellule_3G',"delaycellule_4G",'nbrecellule_2G', 'nbrecellule_3G', 'nbrecellule_4G',
        
        "trafic_voix_2G",	"trafic_voix_3G",	"trafic_voix_4G",	"trafic_data_2G",	"trafic_data_3G",	"trafic_data_4G",
        'cellules_2g_congestionnees', 'cellules_2g', 'cellules_3g_congestionnees', 'cellules_3g', 'cellules_4g_congestionnees', 'cellules_4g',"avg_cssr_cs_2G"	,"avg_cssr_cs_3G"]]
    
    
    logging.info("final columns renamed")  
    
    df_final.columns = ['mois','mois1', 'code_oci',
                                'site',
                                'autre_code',
                                'longitude',
                                'latitude',
                                'type_du_site',
                                'statut',
                                'localisation',
                                'commune',
                                'departement',
                                'region',
                                'partenaires',
                                'proprietaire',
                                'gestionnaire',
                                'type_geolocalite',
                                'projet',
                                'clutter',
                                'position_site',
                                'ca_voix',
                                'ca_data',
                                'parc_global',
                                'parc_data',
                                'o_m',
                                'energy',
                                'infra',
                                'maintenance_passive_preventive',
                                'gardes_de_securite',
                                'discount',
                                'volume_discount',
                                'tva',  'opex',
                                'delay_2g',
                                'delay_3g',
                                'delay_4g', 'delaycellule_2g', 'delaycellule_3g',"delaycellule_4g",
                                'nbrecellule_2g',
                                'nbrecellule_3g',
                                'nbrecellule_4g',
                                'trafic_voix_2g',
                                'trafic_voix_3g',
                                'trafic_voix_4g',
                                'trafic_data_2g',
                                'trafic_data_3g',
                                'trafic_data_4g',
                                'cellules_2g_congestionnees',
                                'cellules_2g',
                                'cellules_3g_congestionnees',
                                'cellules_3g',
                                'cellules_4g_congestionnees',
                                'cellules_4g',
                                'avg_cssr_cs_2g',
                                'avg_cssr_cs_3g']
    
    # enrich
    df_final["trafic_voix_total"] = df_final["trafic_voix_2g"]+df_final["trafic_voix_3g"] + df_final["trafic_voix_4g"]
    df_final["trafic_data_total"] = df_final["trafic_data_2g"]+df_final["trafic_data_3g"] + df_final["trafic_data_4g"]

    df_final["ca_total"] = df_final["ca_data"] + df_final["ca_voix"]
    df_final.loc[((df_final.localisation.str.lower()=="abidjan") & (df_final.ca_total>=20000000)) | ((df_final.localisation.str.lower()=="intérieur") & (df_final.ca_total>=10000000)),["segment"]] = "PREMIUM"
    df_final.loc[((df_final.localisation.str.lower()=="abidjan") & ((df_final.ca_total>=10000000) & (df_final.ca_total<20000000) )) | ((df_final.localisation.str.lower()=="intérieur") & ((df_final.ca_total>=4000000) & (df_final.ca_total<10000000))),["segment"]] = "NORMAL"
    df_final.loc[((df_final.localisation.str.lower()=="abidjan") & (df_final.ca_total<10000000)) | ((df_final.localisation.str.lower()=="intérieur") & (df_final.ca_total<4000000)),["segment"]] = "A DEVELOPPER"


    df_final = df_final.loc[:, ['mois',
                                'code_oci',
                                'site',
                                'autre_code',
                                'longitude',
                                'latitude',
                                'type_du_site',
                                'statut',
                                'localisation',
                                'commune',
                                'departement',
                                'region',
                                'partenaires',
                                'proprietaire',
                                'gestionnaire',
                                'type_geolocalite',
                                'projet',
                                'clutter',
                                'position_site',
                                'ca_voix',
                                'ca_data',
                                'parc_global',
                                'parc_data',
                                'o_m',
                                'energy',
                                'infra',
                                'maintenance_passive_preventive',
                                'gardes_de_securite',
                                'discount',
                                'volume_discount',
                                'tva',
                                'opex',
                                'delay_2g',
                                'delay_3g',
                                'delay_4g', 'delaycellule_2g', 'delaycellule_3g',"delaycellule_4g",
                                'nbrecellule_2g',
                                'nbrecellule_3g',
                                'nbrecellule_4g',
                                'trafic_voix_2g',
                                'trafic_voix_3g',
                                'trafic_voix_4g',
                                'trafic_data_2g',
                                'trafic_data_3g',
                                'trafic_data_4g',
                                'cellules_2g_congestionnees',
                                'cellules_2g',
                                'cellules_3g_congestionnees',
                                'cellules_3g',
                                'cellules_4g_congestionnees',
                                'cellules_4g',
                                'avg_cssr_cs_2g',
                                'avg_cssr_cs_3g',
                                'trafic_voix_total',
                                'trafic_data_total',
                                'ca_total',
                                'segment']]
    
    
    
    # pareto
    oneforall = pareto(df_final)
    oneforall["date"] = oneforall["mois"]+"-01"
    oneforall["date"] = pd.to_datetime(oneforall["date"])

    # get thresold
    interco = float(get_thresold("intercos")) /100 #CA-voix 
    impot = float(get_thresold("impot_taxe"))/100 #CA
    frais_dist = float( get_thresold("frais_distribution"))/100 #CA
    seuil_renta = float(get_thresold("seuil_rentabilite"))/100


    oneforall["interco"] = oneforall["ca_voix"]*interco
    oneforall["impot"] = oneforall["ca_total"]*impot
    oneforall["frais_dist"] = oneforall["ca_total"]*frais_dist
    oneforall["opex_total"] = oneforall['opex'] + oneforall["interco"] + oneforall["impot"] + oneforall["frais_dist"]

    oneforall["ebitda"] = oneforall["ca_total"] - oneforall["opex_total"]

    oneforall["rentable"] = (oneforall["ebitda"]/oneforall["opex_total"])>seuil_renta
    oneforall["niveau_rentabilite"] = "NEGATIF"
    oneforall.loc[(oneforall["ebitda"]/oneforall["opex_total"])>0, "niveau_rentabilite"] = "0-80%"
    oneforall.loc[(oneforall["ebitda"]/oneforall["opex_total"])>0.8, "niveau_rentabilite"] = "+80%"

    logging.info("add NUR") 
    

    oneforall["days"] = oneforall["mois"].apply(get_number_days)

    oneforall["nur_2g"] = (100000 * oneforall['nbrecellule_2g'] * oneforall['delaycellule_2g'] )/ (3600*24*oneforall["days"]  * get_number_cellule(oneforall, "cellules_2g") )
    oneforall["nur_3g"] = (100000 * oneforall['nbrecellule_3g'] * oneforall['delaycellule_3g'] )/ (3600*24*oneforall["days"]  * get_number_cellule(oneforall, "cellules_3g") )
    oneforall["nur_4g"] = (100000 * oneforall['nbrecellule_4g'] * oneforall['delaycellule_4g'] )/ (3600*24*oneforall["days"]  * get_number_cellule(oneforall, "cellules_4g") )
    
    oneforall["previous_segment"] = None
    oneforall.reset_index(drop=True,inplace=True)
    if datetime.strptime(date, CONFIG["date_format"]) > datetime.strptime(start_date, CONFIG["date_format"]):
        last = datetime.strptime(date, CONFIG["date_format"]) - timedelta(weeks=4)
        last_filename = get_latest_file(client, "oneforall", prefix = f"{last.year}/{str(last.month).zfill(2)}")
        logging.info(f"read {last_filename}")
        lastoneforall = pd.read_csv(f"s3://oneforall/{last_filename}",
                                storage_options={
                                "key": accesskey,
                                "secret": secretkey,
                "client_kwargs": {"endpoint_url": f"http://{endpoint}"}
                }
                    )
        
        for idx, row in oneforall.iterrows():
            previos_segment = lastoneforall.loc[lastoneforall.code_oci==row["code_oci"], "segment"].values
            print(previos_segment)
            oneforall.loc[idx, "previous_segment"] = previos_segment if len(previos_segment)>0 else None
            print(oneforall.loc[idx, ["code_oci","previous_segment"]])
    return oneforall


def get_last_ofa(client, endpoint: str, accesskey: str, secretkey: str, date: str):
    """ """
    # get files
    date_parts = date.split("-")
    filename = get_latest_file(client, bucket="oneforall", prefix=f"{date_parts[0]}/{date_parts[1]}/{date_parts[2]}")

   
    try:
                    
        df_ = pd.read_csv(f"s3://oneforall/{filename}",
                        storage_options={
                            "key": accesskey,
                            "secret": secretkey,
                            "client_kwargs": {"endpoint_url": f"http://{endpoint}"}
                                    }
                            )
    except Exception as error:
            raise OSError(f"{filename} don't exists in bucket") from error
    
    return df_