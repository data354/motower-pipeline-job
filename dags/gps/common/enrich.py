""" enrich data"""

import pandas as pd
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
  
    """
    try:
        year, month = map(int, mois.split("-"))
    except ValueError:
        return "Invalid input format. Please use format 'YYYY-MM'."
    return calendar.monthrange(year, month)[1]


def get_number_cellule(df, cellule: str):
    # Get the sum of values in the specified cell for the given month
    cellule_total = df[cellule].sum()
    return cellule_total



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
        if past_site == row["CODE_OCI"]:
            df.loc[idx, "PREVIOUS_SEGMENT"] = past_segment
            past_segment = row["SEGMENT"]
        else: 
            past_site = row["CODE_OCI"]
            past_segment = row["SEGMENT"]
            df.loc[idx,"PREVIOUS_SEGMENT"] = None
    return df


def oneforall(client, endpoint:str, accesskey:str, secretkey:str,  date: str, start_date):
    """
       merge all data and generate oneforall
    """
    #get bdd site
    objet = next((table for table in CONFIG["tables"] if table["name"] == "BASE_SITES"), None)
    filename = get_latest_file(client= client, bucket= objet["bucket"], prefix = f"{objet['folder']}-cleaned/{date.split('-')[0]}/{date.split('-')[1]}/{date.split('-')[2]}")
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
    objet = [d for d in CONFIG["tables"] if d["name"] == "caparc"][0]
    
        
    filename = get_latest_file(client, objet["bucket"], prefix = f"{objet['folder']}-cleaned/{date.split('-')[0]}/{date.split('-')[1]}/{date.split('-')[2]}")
       
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

    objet = next((table for table in CONFIG["tables"] if table["name"] == "OPEX_ESCO"), None) 
        
        
    filename = get_latest_file(client, objet["bucket"], prefix = f"{objet['folder']}-cleaned/{date.split('-')[0]}/{date.split('-')[1]}/{date.split('-')[2]}")

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
    filename = get_latest_file(client, objet["bucket"], prefix = f"{objet['folder']}-cleaned/{date.split('-')[0]}/{pre}/{date.split('-')[2]}")

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
    filename = get_latest_file(client, objet["bucket"], prefix = f"{objet['folder']}-cleaned/{date.split('-')[0]}/{date.split('-')[1]}/{date.split('-')[2]}")

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
    filename = get_latest_file(client, objet["bucket"], prefix = f"{objet['folder']}-cleaned/{date.split('-')[0]}/{date.split('-')[1]}/{date.split('-')[2]}")

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
    filename = get_latest_file(client, objet["bucket"], prefix = f"{objet['bucket']}-cleaned/{date.split('-')[0]}/{date.split('-')[1]}/{date.split('-')[2]}")
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
    objet = [d for d in CONFIG["tables"] if "CONGESTION" in d["name"] ][0]
    filename = get_latest_file(client, objet["bucket"], prefix = f"{objet['folder']}-cleaned/{date.split('-')[0]}/{date.split('-')[1]}/{date.split('-')[2]}")
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
    bdd_CA_ihs_esco.loc[bdd_CA_ihs_esco["maintenance passive preventive_x"].isnull(), "maintenance passive preventive_x"] = bdd_CA_ihs_esco["maintenance passive preventive_y"]
    bdd_CA_ihs_esco.loc[bdd_CA_ihs_esco["gardes de securite_x"].isnull(), "gardes de securite_x"] = bdd_CA_ihs_esco["gardes de securite_y"]
    bdd_CA_ihs_esco.loc[bdd_CA_ihs_esco["discount_x"].isnull(), "discount_x"] = bdd_CA_ihs_esco["discount_y"]
    bdd_CA_ihs_esco.loc[bdd_CA_ihs_esco["volume discount_x"].isnull(), "volume discount_x"] = bdd_CA_ihs_esco["volume discount_y"]
    
    
    logging.info("add indisponibilite")
    bdd_CA_ihs_esco_ind = bdd_CA_ihs_esco.merge(indisponibilite, left_on =["code oci"], right_on = ["code_site"], how="left" )

    logging.info("add congestion")
    bdd_CA_ihs_esco_ind_cong = bdd_CA_ihs_esco_ind.merge(cong, left_on =["code oci"], right_on = ["code_site"], how="left" )

    logging.info("add trafic")
    bdd_CA_ihs_esco_ind_cong_trafic = bdd_CA_ihs_esco_ind_cong.merge(trafic, left_on =["code oci"], right_on = ["code_site"], how="left" )
    logging.info("add cssr")
    bdd_CA_ihs_esco_ind_cong_trafic_cssr = bdd_CA_ihs_esco_ind_cong_trafic.merge(cssr, left_on =["code oci"], right_on = ["code_site"], how="left" )

    logging.info("final columns")

    df_final = bdd_CA_ihs_esco_ind_cong_trafic_cssr.loc[:,[ 'mois_x','code oci','site','autre code','longitude', 'latitude',
                                                       'type du site', 'statut','localisation', 'commune', 'departement', 'region',
                                                         'partenaires','proprietaire', 'gestionnaire','type geolocalite',
                                                           'projet', 'clutter', 'position site', 'ca_voix', 'ca_data', 'parc_voix', 'parc_data',
                                                           'o&m_x', 'energy_x', 'infra_x', 'maintenance passive preventive_x',
       'gardes de securite_x', 'discount_x', 'volume discount_x' ,'tva : 18%', "month_total",'delay_2g', 'delay_3g', 'delay_4g','nbrecellule_2g', 'nbrecellule_3g', 'nbrecellule_4g',
        
        "trafic_voix_2g",	"trafic_voix_3g",	"trafic_voix_4g",	"trafic_data_2g",	"trafic_data_3g",	"trafic_data_4g",
        'cellules_2g_congestionnees', 'cellules_2g', 'cellules_3g_congestionnees', 'cellules_3g', 'cellules_4g_congestionnees', 'cellules_4g',"avg_cssr_cs_2g"	,"avg_cssr_cs_3g"]]
    
    
    logging.info("final columns renamed")  
    
    df_final.columns = ['MOIS','MOIS1', 'CODE_OCI','SITE', 'AUTRE_CODE', 'LONGITUDE', 'LATITUDE',
       'TYPE_DU_SITE', 'STATUT', 'LOCALISATION', 'COMMUNE', 'DEPARTEMENT', 'REGION',
       'PARTENAIRES', 'PROPRIETAIRE', 'GESTIONNAIRE', 'TYPE_GEOLOCALITE',
       'PROJET', 'CLUTTER', 'POSITION_SITE', 'CA_VOIX', 'CA_DATA', 'PARC_GLOBAL',
       'PARC_DATA', 'O_M', 'Energy', 'Infra', 'Maintenance_Passive_preventive',
       'Gardes_de_securite', 'Discount', 'Volume_discount' ,'TVA', 'OPEX',  'delay_2G', 'delay_3G', 'delay_4G', 'nbrecellule_2G', 'nbrecellule_3G', 'nbrecellule_4G',
        "trafic_voix_2G",	"trafic_voix_3G",	"trafic_voix_4G",	"trafic_data_2G",	"trafic_data_3G","trafic_data_4G", 'cellules_2g_congestionnees', 'cellules_2g', 'cellules_3g_congestionnees', 'cellules_3g', 'cellules_4g_congestionnees', 'cellules_4g',
        "avg_cssr_cs_2G"	,"avg_cssr_cs_3G"]
    
    df_final["trafic_voix_total"] = df_final["trafic_voix_2G"]+df_final["trafic_voix_3G"] + df_final["trafic_voix_4G"]
    df_final["trafic_data_total"] = df_final["trafic_data_2G"]+df_final["trafic_data_3G"] + df_final["trafic_data_4G"]

    df_final["CA_TOTAL"] = df_final["CA_DATA"] + df_final["CA_VOIX"]
    df_final.loc[((df_final.LOCALISATION.str.lower()=="abidjan") & (df_final.CA_TOTAL>=20000000)) | ((df_final.LOCALISATION.str.lower()=="intérieur") & (df_final.CA_TOTAL>=10000000)),["SEGMENT"]] = "PREMIUM"
    df_final.loc[((df_final.LOCALISATION.str.lower()=="abidjan") & ((df_final.CA_TOTAL>=10000000) & (df_final.CA_TOTAL<20000000) )) | ((df_final.LOCALISATION.str.lower()=="intérieur") & ((df_final.CA_TOTAL>=4000000) & (df_final.CA_TOTAL<10000000))),["SEGMENT"]] = "NORMAL"
    df_final.loc[((df_final.LOCALISATION.str.lower()=="abidjan") & (df_final.CA_TOTAL<10000000)) | ((df_final.LOCALISATION.str.lower()=="intérieur") & (df_final.CA_TOTAL<4000000)),["SEGMENT"]] = "A DEVELOPPER"


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
                                'delay_4g',
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
    oneforall["date"] = oneforall["MOIS"]+"-01"
    oneforall["date"] = pd.to_datetime(oneforall["date"])

    interco = 0.139 #CA-voix 
    impot = 0.116 #CA
    frais_dist =  0.09 #CA
    seuil_renta = 0.8


    oneforall["INTERCO"] = oneforall["CA_VOIX"]*interco
    oneforall["IMPOT"] = oneforall["CA_TOTAL"]*impot
    oneforall["FRAIS_DIST"] = oneforall["CA_TOTAL"]*frais_dist
    oneforall["OPEX_TOTAL"] = oneforall['OPEX'] + oneforall["INTERCO"] + oneforall["IMPOT"] + oneforall["FRAIS_DIST"]

    oneforall["EBITDA"] = oneforall["CA_TOTAL"] - oneforall["OPEX_TOTAL"]

    oneforall["RENTABLE"] = (oneforall["EBITDA"]/oneforall["OPEX_TOTAL"])>seuil_renta
    oneforall["NIVEAU_RENTABILITE"] = "NEGATIF"
    oneforall.loc[(oneforall["EBITDA"]/oneforall["OPEX_TOTAL"])>0, "NIVEAU_RENTABILITE"] = "0-80%"
    oneforall.loc[(oneforall["EBITDA"]/oneforall["OPEX_TOTAL"])>0.8, "NIVEAU_RENTABILITE"] = "+80%"

    logging.info("add NUR") # a modifier
    

    oneforall["days"] = oneforall["MOIS"].apply(get_number_days)

    oneforall["nur_2g"] = (100000 * oneforall['nbrecellule_2g'] * oneforall['delaycellule_2g'] )/ (3600*24*oneforall["days"]  * get_number_cellule(oneforall, "cellules_2g") )
    oneforall["nur_3g"] = (100000 * oneforall['nbrecellule_3g'] * oneforall['delaycellule_3g'] )/ (3600*24*oneforall["days"]  * get_number_cellule(oneforall, "cellules_3g") )
    oneforall["nur_4g"] = (100000 * oneforall['nbrecellule_4g'] * oneforall['delaycellule_4g'] )/ (3600*24*oneforall["days"]  * get_number_cellule(oneforall, "cellules_4g") )
    
    oneforall["previous_segment"] = None
    oneforall.reset_index(drop=True,inplace=True)
    if datetime.strptime(date, "%Y-%m-%d") > datetime.strptime(start_date, "%Y-%m-%d"):
        last = datetime.strptime(date, "%Y-%m-%d") - timedelta(weeks=4)
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
            previos_segment = lastoneforall.loc[lastoneforall.CODE_OCI==row["code_oci"], "segment"].values
            print(previos_segment)
            oneforall.loc[idx, "previous_segment"] = previos_segment if len(previos_segment)>0 else None
            print(oneforall.loc[idx, ["code_oci","previous_segment"]])
        # big = pd.concat([lastoneforall, oneforall])
        # big = big.sort_values(["CODE_OCI", "MOIS"])
        # past_site = None
        # past_segment = None
        # big["PREVIOUS_SEGMENT"] = None
        # for idx, row in big.iterrows():
        #     if past_site == row["CODE_OCI"]:
        #         big.loc[idx, "PREVIOUS_SEGMENT"] = past_segment
        #         past_segment = row["SEGMENT"]
        #     else: 
        #         past_site = row["CODE_OCI"]
        #         past_segment = row["SEGMENT"]
        #         big.loc[idx,"PREVIOUS_SEGMENT"] = None
        #final = prev_segment(big)
        #big = big.loc[big.MOIS.isin(oneforall.MOIS.unique()), :]
        #big = big.loc[big.PREVIOUS_SEGMENT.notnull()]
    #     return final.loc[:,['MOIS', 'CODE_OCI','SITE', 'AUTRE_CODE', 'LONGITUDE', 'LATITUDE',
    #    'TYPE_DU_SITE', 'STATUT', 'LOCALISATION', 'COMMUNE', 'DEPARTEMENT', 'REGION',
    #    'PARTENAIRES', 'PROPRIETAIRE', 'GESTIONNAIRE', 'TYPE_GEOLOCALITE',
    #    'PROJET', 'CLUTTER', 'POSITION_SITE', 'CA_VOIX', 'CA_DATA', 'PARC_GLOBAL',
    #    'PARC_DATA', 'O_M', 'Energy', 'Infra', 'Maintenance_Passive_preventive',
    #    'Gardes_de_securite', 'Discount', 'Volume_discount' ,'TVA', 'OPEX',  'delay_2G', 'delay_3G', 'delay_4G', 'nbrecellule_2G', 'nbrecellule_3G', 'nbrecellule_4G', "Cellules_2G_congestionnees", "Cellules_3G_congestionnees", "Cellules_4G_congestionnees",
    #     "trafic_voix_2G",	"trafic_voix_3G",	"trafic_voix_4G",	"trafic_data_2G",	"trafic_data_3G","trafic_data_4G","avg_cssr_cs_2G"	,"avg_cssr_cs_3G", "trafic_voix_total", "trafic_data_total", "CA_TOTAL", "SEGMENT",	"PARETO",	"INTERCO",	"IMPOT",	"FRAIS_DIST",	"OPEX_TOTAL",	"EBITDA",	"RENTABLE",	"NIVEAU_RENTABILITE",	"days",	"NUR_2G",	"NUR_3G",	"NUR_4G",	"PREVIOUS_SEGMENT"]]
        #return big.drop(big.columns[0], axis=1)
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