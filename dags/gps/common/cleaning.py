""" CLEANING COMMON"""
import logging
from typing import List
from copy import deepcopy
import calendar
from io import BytesIO
import pandas as pd
from minio import Minio
from copy import deepcopy
from unidecode import unidecode
from gps import CONFIG
from gps.common.rwminio import save_minio, get_latest_file, get_files, read_file
from gps.common.data_validation import validate_column
from gps.common.alerting import get_receivers

def check_columns(df_: pd.DataFrame, required_cols:List):
    """
    Checking for missing columns
    """
    df_.columns = df_.columns.str.lower().str.strip().map(unidecode)
    missing_cols = set(required_cols) - set(df_.columns)
    if missing_cols:
        raise ValueError(f"Missing columns: {', '.join(missing_cols)}")
    logging.info("All required columns are presents")

def clean_dataframe(df_, cols_to_trim, subset_unique, subset_na)-> pd.DataFrame:
    """
      trim some cols, drop duplicates, dropna
    """
    df_[cols_to_trim] = df_[cols_to_trim].apply(lambda x: x.str.strip(), axis=0)
    df_.drop_duplicates(subset=subset_unique, inplace=True, keep="first")
    df_.dropna(subset=subset_na, inplace=True)
    return df_

def process_code_oci_annexe(col: str):
    """
        get the good code oci
    """
    values = col.split(' ')
    if len(values) > 1:
        return values[1]
    else:
        return col

def clean_base_sites(client: Minio, date: str) -> None:
    """
       clean  base sites file:
       Args:
        - client: Minio client
        - endpoint: Minio endpoint
        - accesskey: Minio accesskey
        - secretkey: Minio secretkey
        - date: execution date (provide by airflow)
      Return:
        None
    """
    # Get the table object
    table_obj = next((table for table in CONFIG["tables"] if table["name"] == "BASE_SITES"), None)
    if not table_obj:
        raise ValueError("Table BASE_SITES not found.")
    
    # Get filename
    date_parts = date.split("-")
    filename = get_latest_file(client=client, bucket=table_obj["bucket"], prefix="{}/{}_{}".format(table_obj['folder'], table_obj['folder'], date_parts[0]+date_parts[1]))
    logging.info("Reading %s", filename)
    
    # Read file from minio
    if filename is None:
        raise RuntimeError(f"No file for {date}")

    logging.info(" Read file  %s", filename)
    df_ = read_file(client, table_obj["bucket"], filename)
    
    # Check columns
    logging.info("Checking columns")
    required_cols = table_obj["columns"]
    check_columns(df_, required_cols)
    # Cleaning data
    logging.info("Cleaning data")
    df_["mois"] = f"{date_parts[0]}-{date_parts[1]}"
    cols_to_trim = ["code oci", "autre code"]
    subset_na = subset_unique = ["code oci"]
    df_ = clean_dataframe(df_, cols_to_trim, subset_unique, subset_na)
    df_["code oci id"] = df_["code oci"].str.replace("OCI", "").astype("float64")
    #df_ = df_.loc[(df_["statut"].str.lower() == "service") & (df_["position site"].str.lower().isin(["localité", "localié"])), table_obj["columns"] + ["code oci id", "mois"]]
    # Save cleaned data to minio
    logging.info("Saving data to minio")
    save_minio(client, table_obj["bucket"], date, df_, f"{table_obj['folder']}-cleaned") 


def cleaning_esco(client,  date: str)-> None:
    """
    Clean opex esco file
       Args:
        - client: Minio client
        - endpoint: Minio endpoint
        - accesskey: Minio accesskey
        - secretkey: Minio secretkey
        - date: execution date (provide by airflow)
      Return:
        None
    """
    objet = next((d for d in CONFIG["tables"] if d["name"] == "OPEX_ESCO"), None)
    if objet is None:
        raise ValueError("Table OPEX_ESCO not found in CONFIG.")
    
    date_parts = date.split("-")
    filename = get_latest_file(client, objet["bucket"], prefix="{}/{}_{}".format(objet['folder'], objet['folder'], date_parts[0]+date_parts[1] ))
    if filename is  None:
        raise RuntimeError(f"No file for {date}")

    df_ = read_file(client, objet["bucket"], filename, "Fichier_de_calcul", 3)
    df_.columns = df_.columns.str.lower().map(unidecode)
    if "volume discount" not in df_.columns:
        df_["volume discount"] = 0
    required_cols = objet["columns"]
    check_columns(df_, required_cols)
    # Clean columns
    logging.info("clean columns")
    cols_to_trim = ["code site oci", "code site"]
    subset_na=["code site oci","total redevances ht" ]
    subset_unique = ["code site oci"]
    df_["mois"] = date_parts[0]+"-"+date_parts[1]
    df_ = clean_dataframe(df_, cols_to_trim, subset_unique, subset_na)
    ## get annexe if exists
    objet = next((d for d in CONFIG["tables"] if d["name"] == "ANNEXE_OPEX_ESCO"), None)
    if objet is None:
        raise ValueError("Table ANNEXE_OPEX_ESCO not found in CONFIG.")
    
        
    filename = get_latest_file(client, objet["bucket"], prefix="{}/{}_{}".format(objet['folder'], objet['folder'],date_parts[0]+date_parts[1] ))
    data = deepcopy(df_)
    if filename is not None:
        
        logging.info("add annexe")
        annexe = read_file(client=client, bucket_name=objet['bucket'], object_name=filename, header_num=3, sheet_name="Fichier_de_calcul")
        annexe.columns = annexe.columns.str.lower().map(unidecode)
        annexe.dropna(subset = ["code site oci", "code site"], inplace = True)
        annexe["code site oci"] = annexe["code site oci"].apply(process_code_oci_annexe)
        ## concat df_ and annexe
        logging.info("check columns")
        required_cols = objet["columns"]
        check_columns(annexe, required_cols)
        # Clean columns
        logging.info("clean columns")
        cols_to_trim = ["code site oci", "code site"]
        subset_na=["code site oci","total redevances ht" ]
        subset_unique = ["code site oci"]
        annexe["mois"] = date_parts[0]+"-"+date_parts[1]
        annexe = clean_dataframe(annexe, cols_to_trim, subset_unique, subset_na)
        data = pd.concat([data, annexe])

    logging.info("Saving to minio")
    save_minio(client, objet["bucket"], date, data, "OPEX_ESCO-cleaned")

    

def cleaning_ihs(client, date: str)-> None:
    """
    Clean opex esco file
    Args:
        - client: Minio client
        - date: execution date (provided by airflow)
    Return:
        None
    """
    date_parts = date.split("-")
    acceptable_months = ["01", "04", "07", "10"]
    if date_parts[1] not in acceptable_months:
        return
    
    # Retrieving object from CONFIG
    objet = next((d for d in CONFIG["tables"] if d["name"] == "OPEX_IHS"), None)
    if objet is None:
        raise ValueError("Table OPEX_IHS not found in CONFIG.")
    
    # Retrieving the latest file and reading it
    filename = get_latest_file(client, objet["bucket"], prefix="{}/{}_{}".format(objet['folder'], objet['folder'], date_parts[0] + date_parts[1]))
    logging.info("read file %s", filename)
    if filename is None:
        raise RuntimeError(f"No file for {date}")
    
    excel = read_file(client, objet["bucket"], filename, None)
    data = pd.DataFrame()
    
    for sheet in objet["sheets"]:
        matching_sheets = [s for s in excel.keys() if s.find(sheet) != -1]
        
        for sh_ in matching_sheets:
            logging.info("read %s sheet %s", filename, sh_)
            header = 14 if sh_.find("OCI-COLOC") != -1 else 15
            df_ = excel[sh_]
            df_.columns = df_.iloc[header-1]
            df_ = df_.iloc[header:]
            df_.columns = df_.columns.str.lower()
            
            is_bpci_22 = sh_.find("OCI-MLL BPCI 22") == -1
            columns_to_check = ['site id ihs', 'site name', 'category', 'trimestre ht'] if is_bpci_22 else objet["columns"]
            missing_columns = set(columns_to_check) - set(df_.columns)
            
            if missing_columns:
                raise ValueError(f"missing columns {', '.join(missing_columns)} in sheet {sh_} of file {filename}")
            
            df_ = df_.loc[:, ['site id ihs', 'site name', 'category', 'trimestre ht']] if is_bpci_22 else df_.loc[:, objet['columns']]
            
            if is_bpci_22:
                df_['trimestre ht'] = df_['trimestre ht'].astype("float")
            else:
                df_['trimestre 1 - ht'] = df_['trimestre 1 - ht'].astype("float")
            
            df_["month_total"] = df_['trimestre ht'] / 3 if is_bpci_22 else df_['trimestre 1 - ht'] / 3
            data = pd.concat([data, df_])
    
    logging.info("clean columns")
    
    cols_to_trim = ['site id ihs']
    subset_unique = ["site id ihs", "mois"]
    subset_na = ['site id ihs', "trimestre ht"]
    
    data["mois"] = date_parts[0] + "-" + date_parts[1]
    data.loc[data["trimestre ht"].isna(), "trimestre ht"] = data.loc[data["trimestre 1 - ht"].notna(), "trimestre 1 - ht"]
    data = clean_dataframe(data, cols_to_trim, subset_unique, subset_na)
    data_final = pd.concat([data] + [deepcopy(data).assign(mois=date_parts[0] + "-" + str(int(date_parts[1]) + i).zfill(2)) for i in range(1, 3)])
    data_final = data_final.reset_index(drop=True)
    
    logging.info("Add breakout data")
    
    # Download esco to make ratio
    esco_objet = next((d for d in CONFIG["tables"] if d["name"] == "OPEX_ESCO"), None)
    filename = get_latest_file(client, esco_objet["bucket"], prefix=f"{esco_objet['folder']}-cleaned/{date_parts[0]}/{date_parts[1]}/{date_parts[2]}")
    
    if filename is None:
        raise RuntimeError(f"No esco for {date}")
    
    esco = read_file(client, esco_objet["bucket"], filename, sep=',')
    esco.loc[esco["discount"].isnull(), "discount"] = 0
    esco.loc[esco["volume discount"].isnull(), "volume discount"] = 0
    esco["opex_without_discount"] = esco["total redevances ht"] + esco["discount"] + esco["volume discount"]
    esco = esco.groupby("mois").sum()
    esco = esco.loc[:, ["o&m", 'energy', "infra", "maintenance passive preventive", "gardes de securite", "discount", "volume discount", "opex_without_discount"]]
    ratio = esco.loc[:, ["o&m", 'energy', "infra", "maintenance passive preventive", "gardes de securite", "discount"]] / esco["opex_without_discount"].values[0]
    
    data_final["discount"] = 0
    data_final["volume discount"] = 0
    data_final["o&m"] = ratio["o&m"].values * data_final["month_total"]
    data_final["energy"] = ratio["energy"].values * data_final["month_total"]
    data_final["infra"] = ratio["infra"].values * data_final["month_total"]
    data_final["maintenance passive preventive"] = ratio["maintenance passive preventive"].values * data_final["month_total"]
    data_final["gardes de securite"] = ratio["gardes de securite"].values * data_final["month_total"]
    
    logging.info("save to minio")
    save_minio(client, objet["bucket"], date, data_final, f'{objet["folder"]}-cleaned')



def cleaning_ca_parc(client, date: str, smtp_host, smtp_port, smtp_user) -> None:
    """
    cleaning CA & Parc
    """
    objet = next((table for table in CONFIG["tables"] if table["name"] == "caparc"), None)
    if not objet:
        raise ValueError("Table caparc not found.")
    
    # Check if the bucket for the alarm table exists
    if not client.bucket_exists(objet["bucket"]):
        raise ValueError(f"Bucket {objet['bucket']} does not exist.") 
    
    date_parts = date.split("-")
    filenames = get_files(client, objet["bucket"], prefix=f"{objet['folder']}/{date_parts[0]}/{date_parts[1]}")
    number_days = calendar.monthrange(int(date.split("-")[0]), int(date.split("-")[1]))[1]
    if len(filenames) != number_days:
        raise RuntimeError(f"We need {number_days} files for {date} but we have {len(filenames)}")
    
    data_frames = []
    for filename in filenames:
        df_ = read_file(client=client, bucket_name=objet['bucket'], object_name=filename, sep=",")
        df_.columns = df_.columns.str.lower()
        data_frames.append(df_)
    data = pd.concat(data_frames)
    
    cols_to_trim = ["id_site"]
    data[cols_to_trim] = data[cols_to_trim].astype("str").apply(lambda x: x.str.strip())
    
    data = data.sort_values("day_id")
    
    # DATA VALIDATION
    logging.info("DATA VALIDATION")
    df_for_validation = data.groupby("day_id").agg({'ca_voix': 'sum', 'ca_data': 'sum','parc': 'sum', 'parc_data': 'sum', "parc_2g": 'sum',
          "parc_3g": 'sum',
          "parc_4g": 'sum',
          "parc_5g": 'sum',
          "parc_other": 'sum', 
          "ca_total": 'sum'}).reset_index(drop=False)
    
    logging.info('DAILY KPI - %s', df_for_validation.to_string())
    logging.info("Le CA mensuel est de %s", str(df_for_validation['ca_total'].sum()))
    
    for col in ["ca_voix", "ca_data", "parc", "parc_data"]:
        validate_column(df=df_for_validation, file_type="CAPARC", col=col, host=smtp_host, port=smtp_port, user=smtp_user, receivers=get_receivers("CA_SITES"))
    
    logging.info("validations are ok.")
    
    # generate final data
    logging.info("generate and save final data.")
    data = data.groupby(["id_site"]).agg({'ca_voix': 'sum', 'ca_data': 'sum','parc': 'last', 'parc_data': 'last', "parc_2g": 'last',
          "parc_3g": 'last',
          "parc_4g": 'last',
          "parc_5g": 'last',
          "parc_other": 'last',
          "ca_total": 'sum',
          "trafic_voix_in": 'sum',
          "trafic_data_in": 'sum'}).reset_index(drop=False)
    
    logging.info("Start to save data")
    save_minio(client, objet["bucket"], date, data, f'{objet["folder"]}-cleaned')



def cleaning_traffic(client, date: str):
    """
    Cleans traffic files
    Args:
        - client: Minio client
        - date: execution date (provided by airflow)
    Return:
        None
    """
     # Find the required object in the CONFIG dictionary
    objet = next((table for table in CONFIG["tables"] if table["name"] == "hourly_datas_radio_prod"), None)
    if not objet:
        raise ValueError("Table hourly_datas_radio_prod not found.")
    
    # Split the date into parts
    date_parts = date.split("-")
     # Get all files for the given date
    filenames = get_files(client, objet["bucket"], f"{objet['folder']}/{date_parts[0]}/{date_parts[1]}")
     # Read and concatenate all files into a single DataFrame
    data_frames = []
    for filename in filenames:
        df_ = read_file(client=client,bucket_name=objet['bucket'], object_name=filename, sep=",")
        df_.columns = df_.columns.str.lower()
        data_frames.append(df_)
    data = pd.concat(data_frames)
     # Trim whitespace from certain columns
    cols_to_trim = ["code_site"]
    data[cols_to_trim] = data[cols_to_trim].apply(lambda x: x.str.strip())
     # Convert date_jour column to string and add a new column for month
    data.date_jour = data.date_jour.astype("str")
    data["mois"] = data.date_jour.str[:4].str.cat(data.date_jour.str[4:6], "-")
     # Group by month, code_site, and techno and sum traffic_voix and traffic_data
    data = data[["mois", "code_site", "techno", "trafic_voix", "trafic_data"]]
    data = data.groupby(["mois", "code_site", "techno"]).sum()
    data = data.unstack()
    data.columns = ["_".join(d) for d in data.columns]
    data.reset_index(drop=False, inplace=True)
     # Save the cleaned DataFrame to Minio
    save_minio(client, objet["bucket"], date, data, f'{objet["folder"]}-cleaned')

def cleaning_trafic_v2(client, date: str):
    """
    Cleans traffic files
    Args:
        - client: Minio client
        - endpoint: Minio endpoint
        - accesskey: Minio access key
        - secretkey: Minio secret key
        - date: execution date (provided by airflow)
    Return:
        None
    """
     # Find the required object in the CONFIG dictionary
    objet = next((table for table in CONFIG["tables"] if table["name"] == "ks_tdb_radio_drsi"), None)
    if not objet:
        raise ValueError("Table ks_tdb_radio_drsi not found.")
     
     # Split the date into parts
    date_parts = date.split("-")
    filename = get_latest_file(client, objet["bucket"], prefix = f"{objet['folder']}/{date_parts[0]}/{date_parts[1]}/{date_parts[2]}")
    if filename is not None:
        trafic = read_file(client=client,bucket_name=objet['bucket'], object_name=filename, sep="," )
        trafic.columns = trafic.columns.str.lower()
        trafic = trafic.loc[trafic.techno != "4G_TDD", :]
        try:
            trafic["trafic_data_go"] = trafic["trafic_data_go"].str.replace(",", ".").astype("float")
            trafic["trafic_voix_erl"] = trafic["trafic_voix_erl"].str.replace(",", ".").astype("float")
        except AttributeError :
            pass
        trafic["mois"] = trafic["date_id"].str[:-3]
        trafic = trafic[["mois", "id_site", "trafic_data_go", "trafic_voix_erl", "nbre_cellule", "nbre_cellule_congestionne", "techno" ]]
        trafic = trafic.groupby(["mois", "id_site", "techno"]).sum()
        trafic = trafic.unstack()
        trafic.columns = ["_".join(d) for d in trafic.columns]
        trafic.reset_index(drop=False, inplace=True)
     # Save the cleaned dataFrame to Minio
        return trafic
    return pd.DataFrame()





def cleaning_cssr(client,  date: str):
    """
    Cleans cssr files
    Args:
        - client: Minio client
        - endpoint: Minio endpoint
        - accesskey: Minio access key
        - secretkey: Minio secret key
        - date: execution date (provided by airflow)
    Return:
        None
    """
    objet_2g = next((table for table in CONFIG["tables"] if table["name"] == "Taux_succes_2g"), None)
    if not objet_2g:
        raise ValueError("Table Taux_succes_2 not found.")
        # Check if bucket exists
    if not client.bucket_exists(objet_2g["bucket"]):
        raise ValueError(f"Bucket {objet_2g['bucket']} does not exist.") 
    
    objet_3g = next((table for table in CONFIG["tables"] if table["name"] == "Taux_succes_3g"), None)
    if not objet_3g:
        raise ValueError("Table Taux_succes_3 not found.")
        # Check if bucket exists
    
    date_parts = date.split("-")
    filenames = get_files(client, objet_2g["bucket"], prefix = f"{objet_2g['folder']}/{date_parts[0]}/{date_parts[1]}") + get_files(client, objet_3g["bucket"], prefix = f"{objet_3g['folder']}/{date_parts[0]}/{date_parts[1]}")
    data_frames = []
    for filename in filenames:
        df_ = read_file(client=client, bucket_name=objet_2g['bucket'], object_name=filename, sep=",")
        data_frames.append(df_)
    cssr = pd.concat(data_frames, ignore_index=True)
    logging.info("start to clean data")
    cssr.date_jour = cssr.date_jour.astype("str")
    cssr = cssr.drop_duplicates(["date_jour",	"code_site", "techno"], keep="first")
    cssr = cssr.loc[:,["date_jour",	"code_site", "avg_cssr_cs"	,	"techno"] ].dropna(subset=["code_site", "avg_cssr_cs"	,	"techno"])
    cssr = cssr.groupby(["date_jour",	"code_site","techno"	]).sum()
    cssr = cssr.unstack()
    cssr.columns = ["_".join(d) for d in cssr.columns]
    cssr = cssr.reset_index(drop=False)
    cssr["MOIS"] =  cssr.date_jour.str[:4].str.cat(cssr.date_jour.str[4:6], "-" )
    cssr = cssr.groupby(["MOIS", "code_site"]).mean()
    cssr = cssr.reset_index(drop=False)
    logging.info("start to save data")
    save_minio(client, objet_2g["bucket"], date, cssr, f'{objet_2g["bucket"]}-cleaned')


# clean congestion

def cleaning_congestion(client, date: str):
    """
    Cleans congestion files
    Args:
        - client: Minio client
        - endpoint: Minio endpoint
        - accesskey: Minio access key
        - secretkey: Minio secret key
        - date: execution date (provided by airflow)
    Return:
        None
    """
    objet = next((table for table in CONFIG["tables"] if table["name"] == "CONGESTION"), None)
    if not objet:
        raise ValueError("Table CONGESTION not found.")
        
        
    
    date_parts = date.split("-")
    filename = get_latest_file(client=client, bucket=objet["bucket"], prefix=f"{objet['folder']}/{objet['folder']}_{date_parts[0]}{date_parts[1]}")
    logging.info("Reading %s", filename)
    
    # Read file from minio
    if filename is None:
        raise RuntimeError(f"No data for {date}")
    
    df_ = read_file(client=client, bucket_name=objet['bucket'], object_name=filename)
    logging.info("Checking columns")
    required_cols = objet["columns"]
    check_columns(df_, required_cols)
    
    # Clean data
    logging.info("Cleaning data")
    
    df_["mois"] = f"{date_parts[0]}-{date_parts[1]}"
    cols_to_trim = ["code_site"]
    df_[cols_to_trim] = df_[cols_to_trim].apply(lambda x: x.str.strip())
    df_.dropna(subset=["code_site"], inplace=True)
    
    df_ = df_.drop_duplicates(subset=["code_site"])
    
    df_ = df_.groupby(["mois", "code_site"]).agg({
        "cellules_2g": "sum",
        "cellules_2g_congestionnees": "sum",
        "cellules_3g": "sum",
        "cellules_3g_congestionnees": "sum",
        "cellules_4g": "sum",
        "cellules_4g_congestionnees": "sum"
    }).reset_index()
    
    df_[["cellules_4g_congestionnees", "cellules_2g_congestionnees", "cellules_3g_congestionnees"]] = df_[["cellules_4g_congestionnees", "cellules_2g_congestionnees", "cellules_3g_congestionnees"]].fillna(value=0)
    
    save_minio(client, objet["bucket"], date, df_, f'{objet["folder"]}-cleaned')