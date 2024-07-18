""" enrich data"""
import numpy as np
import pandas as pd
import requests
import calendar
from datetime import datetime, timedelta
from minio import Minio
from copy import deepcopy
from gps import CONFIG
from gps.common.rwminio import  get_latest_file, read_file
import logging


################################## joindre les tables
def get_number_days(mois: str):
    """
       get number of days in month
    """
    try:
        year, month = map(int, mois.split("-"))
        if month < 1 or month > 12:
            raise ValueError("Month value must be between 1 and 12.")
        if year < 0:
            raise ValueError("Year value must be positive.")
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
    add new colone to dataframe, this colone containt the cumulate sum of ca_total
    """
    total = np.sum(dataframe["ca_total"].values)
    sorted_indices = np.argsort(dataframe["ca_total"].values)[::-1]
    dataframe = dataframe.iloc[sorted_indices]
    dataframe["sommecum"] = np.cumsum(dataframe["ca_total"].values)
    dataframe["pareto"] = dataframe["sommecum"] < total * 0.8
    dataframe = dataframe.drop(columns=["sommecum"])
    return dataframe

def compute_evolution(row):
    """
    compute evolution
    """
    if (row["previous_segment"] is None) or (row["segment"] is None):
        row["evolution_segment"] = None
    dummy = {"A DEVELOPPER": 0, "NORMAL": 1, "PREMIUM": 2}
    if dummy[row["segment"]] == dummy[row["previous_segment"]]:
        row["evolution_segment"] = 0
    elif dummy[row["segment"]]  > dummy[row["previous_segment"]]:
        row["evolution_segment"] = 1
    elif dummy[row["segment"]] < dummy[row["previous_segment"]]:
        row["evolution_segment"] = -1
    return row

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
        raise ValueError(f"thresold of type {code} not available")
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
    
    filename = get_latest_file(client= client, bucket= objet["bucket"], prefix = f"{objet['folder']}-cleaned/{date_parts[0]}/{date_parts[1]}/{date_parts[2]}")
    if filename is not None:
        bdd = read_file(client=client,bucket_name=objet['bucket'], object_name=filename , sep=',')
        
  
    #get CA
    objet = next((table for table in CONFIG["tables"] if table["name"] == "caparc"), None)
    if objet is None:
        raise ValueError("Table 'caparc' not found in configuration")
        
    filename = get_latest_file(client, objet["bucket"], prefix = f"{objet['folder']}-cleaned/{date_parts[0]}/{date_parts[1]}/{date_parts[2]}")
    if filename is not None:
        caparc = read_file(client=client,bucket_name=objet['bucket'], object_name=filename , sep=',')
      
    # get opex esco

    objet = next((table for table in CONFIG["tables"] if table["name"] == "OPEX_ESCO"), None)
    if objet is None:
        raise ValueError("Table 'OPEX_ESCO' not found in configuration")
    
    filename = get_latest_file(client, objet["bucket"], prefix = f"{objet['folder']}-cleaned/{date_parts[0]}/{date_parts[1]}/{date_parts[2]}")
    if filename is not None:
        esco = read_file(client=client,bucket_name=objet['bucket'], object_name=filename, sep="," )

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
    if filename is not None:
        ihs = read_file(client=client,bucket_name=objet['bucket'], object_name=filename , sep=",")

    
    # get trafic_v2
    objet = next((table for table in CONFIG["tables"] if table["name"] == "ks_tdb_radio_drsi"), None)
    if objet is None:
        raise ValueError("Table 'ks_tdb_radio_drsi' not found in configuration")
    filename = get_latest_file(client, objet["bucket"], prefix = f"{objet['folder']}-cleaned/{date_parts[0]}/{date_parts[1]}/{date_parts[2]}")
    if filename is not None:
        trafic2 = read_file(client=client,bucket_name=objet['bucket'], object_name=filename, sep="," )
        
    # merging # join esco and ihs colonnes

    logging.info("merge bdd and CA")
    bdd_ca = bdd.merge(caparc, left_on=["code oci id"], right_on = ["id_site" ], how="left")

    logging.info("add opex")
    bdd_ca_ihs = bdd_ca.merge(ihs, left_on=[ "autre code", "mois"], right_on=[ "site id ihs", "mois"], how="left")
    bdd_ca_ihs_esco = bdd_ca_ihs.merge(esco, left_on=["autre code"], right_on=["code site"], how="left")
    
   
   
   
    
    bdd_ca_ihs_esco.loc[bdd_ca_ihs_esco["total redevances ht"].notnull(), "month_total"] = bdd_ca_ihs_esco["total redevances ht"]
    bdd_ca_ihs_esco.loc[bdd_ca_ihs_esco["o&m_x"].isnull(), "o&m_x"] = bdd_ca_ihs_esco["o&m_y"]
    bdd_ca_ihs_esco.loc[bdd_ca_ihs_esco["energy_x"].isnull(), "energy_x"] = bdd_ca_ihs_esco["energy_y"]
    bdd_ca_ihs_esco.loc[bdd_ca_ihs_esco["infra_x"].isnull(), "infra_x"] = bdd_ca_ihs_esco["infra_y"]
    bdd_ca_ihs_esco.loc[bdd_ca_ihs_esco["maintenance passive preventive_x"].isnull(), "maintenance passive preventive_x"] = bdd_ca_ihs_esco["maintenance passive preventive_y"]
    bdd_ca_ihs_esco.loc[bdd_ca_ihs_esco["gardes de securite_x"].isnull(), "gardes de securite_x"] = bdd_ca_ihs_esco["gardes de securite_y"]
    bdd_ca_ihs_esco.loc[bdd_ca_ihs_esco["discount_x"].isnull(), "discount_x"] = bdd_ca_ihs_esco["discount_y"]
    bdd_ca_ihs_esco.loc[bdd_ca_ihs_esco["volume discount_x"].isnull(), "volume discount_x"] = bdd_ca_ihs_esco["volume discount_y"]
    
    
    logging.info("add trafic v2")
    bdd_ca_ihs_esco_trafic2 = bdd_ca_ihs_esco.merge(trafic2, left_on =["code oci id"], right_on = ["id_site"], how="left" )
    
    logging.info("final columns")
    bdd_ca_ihs_esco_trafic2 = bdd_ca_ihs_esco_trafic2.loc[:, ~bdd_ca_ihs_esco_trafic2.columns.duplicated()]  #suppression des colones en double du aux jointure
    
  
    
    


    df_final = bdd_ca_ihs_esco_trafic2.loc[:,[ 'mois_x','code oci','site','autre code','longitude', 'latitude','partenaires','proprietaire',
                                            'type du site', 'statut','localisation', 'commune', 'departement', 'region','gestionnaire',
                                            'type geolocalite','projet', 'clutter', 'position site', 'ca_voix', 'ca_data', 'ca_total','parc', 'parc_data',
                                            "parc_2g","parc_3g", "parc_4g", "parc_5g", "parc_other",'o&m_x', 'energy_x', 'infra_x',
                                            'maintenance passive preventive_x','gardes de securite_x', 'discount_x', 'volume discount_x' ,'tva : 18%',
                                            "month_total","trafic_data_go_2G", "trafic_data_go_3G", "trafic_data_go_4G", "trafic_voix_erl_2G", 
                                            "trafic_voix_erl_3G", "trafic_voix_erl_4G","nbre_cellule_2G", "nbre_cellule_congestionne_2G","nbre_cellule_3G",
                                            "nbre_cellule_congestionne_3G", "nbre_cellule_4G", "nbre_cellule_congestionne_4G"]]
    
    
    logging.info("final columns renamed")
    
    print(df_final.columns)
    print(df_final.shape)
    df_final.columns = ['mois', 'code_oci','site','autre_code','longitude','latitude','type_du_site','statut','localisation',
                        'commune', 'departement','region','partenaires', 'proprietaire', 'gestionnaire',  'type_geolocalite', 
                        'projet', 'clutter', 'position_site', 'ca_voix','ca_data', 'ca_total', 'parc_global','parc_data', "parc_2g",
                        "parc_3g", "parc_4g",  "parc_5g", "autre_parc",'o_m', 'energie', 'infra', 'maintenance_passive_preventive',
                        'garde_de_securite','discount', 'volume_discount','tva',  'opex_itn',"trafic_data_v2_2g",  "trafic_data_v2_3g",    
                        "trafic_data_v2_4g", "trafic_voix_v2_2g", "trafic_voix_v2_3g", "trafic_voix_v2_4g",
                        "cellules_v2_2g", "cellules_congestionne_v2_2g","cellules_v2_3g", "cellules_congestionne_v2_3g", "cellules_v2_4g", 
                        "cellules_congestionne_v2_4g"]
    
    
    print(df_final.shape)
    # enrich

    df_final.loc[((df_final.localisation.str.lower()=="abidjan") & (df_final.ca_total>=20000000)) | ((df_final.localisation.str.lower()=="intérieur") & (df_final.ca_total>=10000000)),["segment"]] = "PREMIUM"
    df_final.loc[((df_final.localisation.str.lower()=="abidjan") & ((df_final.ca_total>=10000000) & (df_final.ca_total<20000000) )) | ((df_final.localisation.str.lower()=="intérieur") & ((df_final.ca_total>=4000000) & (df_final.ca_total<10000000))),["segment"]] = "NORMAL"
    df_final.loc[((df_final.localisation.str.lower()=="abidjan") & (df_final.ca_total<10000000)) | ((df_final.localisation.str.lower()=="intérieur") & (df_final.ca_total<4000000)),["segment"]] = "A DEVELOPPER"


    df_final = df_final.loc[:, ['mois','code_oci','site','autre_code','longitude','latitude','type_du_site','statut',
                                'localisation','commune','departement','region','partenaires','proprietaire','gestionnaire',
                                'type_geolocalite','projet','clutter', 'position_site','ca_voix','ca_data', 'parc_global',
                                'parc_data',"parc_2g","parc_3g","parc_4g","parc_5g","autre_parc",'o_m', 'energie','infra',    
                                'maintenance_passive_preventive','garde_de_securite','discount','volume_discount','tva',
                                'opex_itn',"trafic_data_v2_2g", "trafic_data_v2_3g", "trafic_data_v2_4g", "trafic_voix_v2_2g",
                                "trafic_voix_v2_3g", "trafic_voix_v2_4g","cellules_v2_2g", "cellules_congestionne_v2_2g","cellules_v2_3g",
                                "cellules_congestionne_v2_3g", "cellules_v2_4g","cellules_congestionne_v2_4g",'ca_total','segment']]
                                
   
    
    
    # pareto
    oneforall = pareto(df_final)

    oneforall["cellules_congestionnees_total_v2"] = oneforall['cellules_congestionne_v2_2g'] +  oneforall['cellules_congestionne_v2_3g'] + oneforall['cellules_congestionne_v2_4g'] ###

    oneforall["cellules_total_v2"] = oneforall['cellules_v2_2g'] + oneforall['cellules_v2_3g'] + oneforall['cellules_v2_4g'] ###


    oneforall["taux_congestion_2g_v2"] = oneforall['cellules_congestionne_v2_2g'] / oneforall['cellules_v2_2g']
    oneforall["taux_congestion_3g_v2"] = oneforall['cellules_congestionne_v2_3g'] / oneforall['cellules_v2_3g']
    oneforall["taux_congestion_4g_v2"] = oneforall['cellules_congestionne_v2_4g'] / oneforall['cellules_v2_4g']
    oneforall["taux_congestion_total_v2"] =  oneforall["taux_congestion_2g_v2"] + oneforall["taux_congestion_3g_v2"] + oneforall["taux_congestion_4g_v2"]



    oneforall["recommandation_v2"] = "Surveillance technique"
    oneforall.loc[(oneforall["taux_congestion_total_v2"]<=0.5), "recommandation_v2"] =  "Surveillance commerciale"
    
    oneforall["arpu"] = oneforall["ca_total"] / oneforall["parc_global"]
    

    oneforall["segmentation_rentabilite_v2"] = 'Unknown'
    oneforall.loc[((oneforall["arpu"]<3000) & (oneforall["taux_congestion_4g_v2"] < 0.15)),"segmentation_rentabilite_v2"] = 'Seg 1'
    oneforall.loc[((oneforall["arpu"]>=3000) & (oneforall["taux_congestion_4g_v2"] < 0.15)),"segmentation_rentabilite_v2"] = 'Seg 2'
    oneforall.loc[((oneforall["arpu"]>=3000) & (oneforall["taux_congestion_4g_v2"] >=0.15)),"segmentation_rentabilite_v2"] = 'Seg 3'
    oneforall.loc[((oneforall["arpu"]<3000 )& (oneforall["taux_congestion_4g_v2"] >= 0.15)),"segmentation_rentabilite_v2"] = 'Seg 4'


    # get thresold
    interco = float(get_thresold("intercos")) /100 #CA-voix 
    impot = float(get_thresold("impot_taxe"))/100 #CA
    frais_dist = float( get_thresold("frais_distribution"))/100 #CA
    seuil_renta = float(get_thresold("seuil_rentabilite"))/100


    oneforall["interco"] = oneforall["ca_voix"]*interco
    oneforall["impot"] = oneforall["ca_total"]*impot
    oneforall["frais_dist"] = oneforall["ca_total"]*frais_dist
    oneforall["opex"] = oneforall['opex_itn'] + oneforall["interco"] + oneforall["impot"] + oneforall["frais_dist"]
    oneforall["autre_opex"] = oneforall["opex"] - oneforall["opex_itn"]
    oneforall["ebitda"] = oneforall["ca_total"] - oneforall["opex"]
    oneforall["marge_ca"] = oneforall["ebitda"]/oneforall["ca_total"]
    oneforall["rentable"] = (oneforall["marge_ca"])>seuil_renta
   
    oneforall["niveau_rentabilite"] = "NEGATIF"
    oneforall.loc[(oneforall["marge_ca"])>0, "niveau_rentabilite"] = "NON RENTABLE"
    oneforall.loc[(oneforall["marge_ca"])>=seuil_renta, "niveau_rentabilite"] = "RENTABLE"

    logging.info("add NUR") 
    

    oneforall["days"] = oneforall["mois"].apply(get_number_days)


    oneforall["previous_segment"] = None
    oneforall["evolution_segment"] = None
    oneforall.reset_index(drop=True,inplace=True)
    #if datetime.strptime(date, CONFIG["date_format"]) > datetime.strptime(start_date, CONFIG["date_format"]):
    last = datetime.strptime(date, CONFIG["date_format"]) - timedelta(weeks=4)
    last_filename = get_latest_file(client, CONFIG["final_bucket"], prefix = f"{CONFIG['final_folder']}/{last.year}/{str(last.month).zfill(2)}")
    if last_filename is not None:
        
        logging.info(f"read {last_filename}")
        lastoneforall = read_file(client=client, bucket_name=CONFIG["final_bucket"],object_name=last_filename, sep="," )
        oneforall = oneforall.merge(lastoneforall[["code_oci","segment"]].rename(columns={"segment", "psegment"}), on=['code_oci'], how="left" ) 
        oneforall["previous_segment"] = oneforall["psegment"]
        logging.info("ADD EVOLUTION SEGMENT")
        oneforall = oneforall.apply(func=compute_evolution, axis=1)
        
    return oneforall


