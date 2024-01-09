"""  DAILY PROCESSING """

import logging
from datetime import datetime
import pandas as pd
import psycopg2
from dateutil import relativedelta
from gps import CONFIG
from gps.common.rwminio import  get_latest_file, read_file
from gps.common.data_validation import validate_column, validate_site_actifs




DAILY_CAPARC_COL = {
    "jour": "jour",
    "code oci": "code_oci",
    "code oci id": "code_oci_id",
    "autre code": "autre_code",
    "clutter": "clutter",
    "commune": "commune",
    "departement": "departement",
    "type du site": "type_du_site",
    "type geolocalite": "type_geolocalite",
    "gestionnaire": "gestionnaire",
    "latitude": "latitude",
    "longitude": "longitude",
    "localisation": "localisation",
    "partenaires": "partenaires",
    "proprietaire": "proprietaire",
    "position site": "position_site",
    "site": "site",
    "statut": "statut",
    "projet": "projet",
    "region": "region",
    "ca_data": "ca_data",
    "ca_voix": "ca_voix",
    "ca_total": "ca_total",
    "parc": "parc_global",
    "parc_data": "parc_data",
    "parc_2g": "parc_2g",
    "parc_3g": "parc_3g",
    "parc_4g": "parc_4g",
    "parc_5g": "parc_5g",
    "parc_other": "autre_parc",
    "trafic_data_in": "trafic_data_in",
    "trafic_voix_in": "trafic_voix_in"
}

def compute_segment(row):
    """
       compute  segment
    """
    if pd.isna(row["ca_norm"]) or pd.isna(row["localisation"]):
        row["segment"] = None
    else:
        row["localisation"] = row["localisation"].lower()
        if row["localisation"] == "abidjan":
            if row["ca_norm"] >= 20000000:
                row["segment"] = "PREMIUM"
            elif row["ca_norm"] >= 10000000:
                row["segment"] = "NORMAL"
            else:
                row["segment"] = "A DEVELOPER"
        elif row["localisation"] == "intérieur":
            if row["ca_norm"] >= 10000000:
                row["segment"] = "PREMIUM"
            elif row["ca_norm"] >= 4000000:
                row["segment"] = "NORMAL"
            else:
                row["segment"] = "A DEVELOPER"
    return row

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

def generate_daily_caparc(client,  smtphost: str, smtpport: int, smtpuser: str, date: str, pghost, pguser, pgpwd, pgdb):
    """
        create motower daily structure
    """

    logging.info("get last modified bdd sites cleaned")
    exec_date = datetime.strptime(date, CONFIG["date_format"])
    table_obj = next((table for table in CONFIG["tables"] if table["name"] == "BASE_SITES"), None)
    if not table_obj:
        raise ValueError("Table BASE_SITES not found.")
     # Check if bucket exists
    if not client.bucket_exists(table_obj["bucket"]):
        raise ValueError(f"Bucket {table_obj['bucket']} does not exist.")
     # Get filename
    filename = get_latest_file(client=client, bucket=table_obj["bucket"], prefix=f"{table_obj['folder']}-cleaned/")
    logging.info("READING %s", filename)
     # Read file from minio
    if filename is not None:
        bdd = read_file(client=client,bucket_name=table_obj['bucket'], object_name=filename , sep=',')
    
    # BDD SITE VALIDATION
    validate_site_actifs(df_bdd_site=bdd, col="code oci", host=smtphost, port=smtpport, user=smtpuser)


    # CA PARC
    logging.info("GET CA PARC FILE OF THE DAY %s", date)
    table_obj = next((table for table in CONFIG["tables"] if table["name"] == "caparc"), None)
    date_parts = date.split("-")
    filename = f"{table_obj['folder']}/{date_parts[0]}/{date_parts[1]}/{date_parts[2]}.csv"
    logging.info("READING %s", filename)
    ca = read_file(client=client, bucket_name=table_obj['bucket'], object_name=filename, sep=',')
    df_for_validation = ca.groupby("day_id").agg({'ca_voix': 'sum', 'ca_data': 'sum', 'parc': 'sum', 'parc_data': 'sum', "parc_2g": 'sum',
                                                  "parc_3g": 'sum', "parc_4g": 'sum', "parc_5g": 'sum', "parc_other": 'sum', "ca_total": 'sum'})
    df_for_validation.reset_index(drop=False, inplace=True)
    logging.info('DAILY KPI - %s', df_for_validation.to_string())
    for col in ["ca_total", "ca_voix", "ca_data", "parc", "parc_data"]:
        validate_column(df=df_for_validation, col=col, host=smtphost, port=smtpport, user=smtpuser, file_type="CAPARC", date=date)
    logging.info("MERGING BDD AND CA")
    bdd_ca = bdd.merge(ca, left_on=["code oci id"], right_on=["id_site"], how="left")
    logging.info("prepare final daily data")
    
    bdd_ca["jour"] = exec_date

    df_final = bdd_ca[list(DAILY_CAPARC_COL.keys())]
    df_final.rename(columns=DAILY_CAPARC_COL, inplace=True)
    df_final["trafic_data_in_mo"] = df_final["trafic_data_in"] / 1000
    df_final["ca_norm"] = None
    df_final["ca_mtd"] = None
    df_final["segment"] = None
    df_final["previous_segment"] = None
    df_final["evolution_segment"] = None

    # GET DATA MONTH TO DAY
    mois = exec_date.month
    conn = psycopg2.connect(host=pghost, database=pgdb, user=pguser, password=pgpwd)
    sql_query =  "select * from motower_daily_caparc where EXTRACT(MONTH FROM jour) = %s AND jour < %s"
    daily_month_df = pd.read_sql_query(sql_query, conn, params=(mois, date))
    month_data = pd.concat([daily_month_df, df_final])
    month_data["jour"] = pd.to_datetime(month_data["jour"])
    month_data["ca_mtd"] = month_data["ca_total"].groupby(['code_oci']).cumsum()
    month_data["ca_norm"] = month_data["ca_mtd"] * 30
    # add segment
    logging.info("ADD  SEGMENT")
    month_data = month_data.apply(f=compute_segment, axis=1)
    # get data of the day
    df_final = month_data.loc[month_data["jour"]==exec_date,:]
    # add previous segement
    logging.info("ADD PREVIOUS SEGMENT")
    lmonth = exec_date - relativedelta.relativedelta(months=1)
    sql_query =  "select * from motower_daily_caparc where  EXTRACT(MONTH FROM jour) = %s and EXTRACT(DAY FROM jour) = %s "
    last_month = pd.read_sql_query(sql_query, conn, params=(lmonth.month,exec_date.day))
    if not last_month.empty:
        df_final = pd.merge(left=df_final,right=last_month[["code_oci", "segment"]].rename(mapper={"segment":"psegment"}), how="left", on="code_oci", validate="one_to_one")
        df_final["previous_segment"] = df_final["psegment"]

        logging.info("ADD EVOLUTION SEGMENT")
        df_final = df_final.apply(f=compute_evolution, axis=1)

    logging.info("DATA VALIDATION AFTER MERGING")
    validate_site_actifs(df_bdd_site=df_final, col="code_oci", host=smtphost, port=smtpport, user=smtpuser)

    df_for_validation = df_final.groupby("jour").aggregate({'ca_voix': 'sum', 'ca_data': 'sum','parc_global': 'sum', 'parc_data': 'sum', "parc_2g": 'sum',
          "parc_3g": 'sum',
          "parc_4g": 'sum',
          "parc_5g": 'sum',
          "autre_parc": 'sum', 
          "ca_total": 'sum'})
    df_for_validation.reset_index(drop=False, inplace=True)

    logging.info('DAILY KPI - %s',df_for_validation.to_string())
    for col in ["ca_total","ca_voix", "ca_data", "parc_global", "parc_data"]:
        validate_column(df=df_for_validation, col=col,  host=smtphost, port= smtpport, user=smtpuser, file_type="CAPARC JOIN TO BDD SITE", date=date)
    return df_final


def cleaning_daily_trafic(client, smtphost: str, smtpport: str, smtpuser: str, date: str):
    """
    Cleans traffic files
    Args:
        - client: Minio client
        - endpoint: Minio endpoint
        - accesskey: Minio access key
        - secretkey: Minio secret key
        - date: execution date (provided by airflow %Y-%m-%d)
    Return:
        pandas.DataFrame: Cleaned traffic DataFrame
    """
     # Find the required object in the CONFIG dictionary
     
    objet = next((table for table in CONFIG["tables"] if table["name"] == "ks_daily_tdb_radio_drsi"), None)
    if not objet:
        raise ValueError("Table ks_daily_tdb_radio_drsi not found.")
     # Check if bucket exists
    if not client.bucket_exists(objet["bucket"]):
        raise ValueError(f"Bucket {objet['bucket']} does not exist.")
     # Split the date into parts
    date_parts = date.split("-")
    filename = get_latest_file(client, objet["bucket"], prefix = f"{objet['folder']}/{date_parts[0]}/{date_parts[1]}/{date_parts[2]}")
    if filename is not None:
        trafic = read_file(client=client, bucket_name=objet['bucket'], object_name=filename, sep=',')

    trafic.columns = trafic.columns.str.lower()
    trafic = trafic.loc[trafic["techno"].isin(["2G", "3G", "4G"]), :]
    try:
        trafic["trafic_data_go"] = trafic["trafic_data_go"].astype("float")
        trafic["trafic_voix_erl"] = trafic["trafic_voix_erl"].astype("float")
    except ValueError:
        trafic["trafic_data_go"] = trafic["trafic_data_go"].str.replace(",", ".").astype("float")
        trafic["trafic_voix_erl"] = trafic["trafic_voix_erl"].str.replace(",", ".").astype("float")
    
    # DATA VALIDATION
    df_to_validate = trafic.groupby("date_id").aggregate({'trafic_data_go': 'sum', 'trafic_voix_erl': 'sum'}).reset_index()
    for col in ["trafic_data_go", "trafic_voix_erl"]:
       validate_column(df=df_to_validate, col=col, host=smtphost, port=smtpport, user=smtpuser, file_type="trafic", date=df_to_validate["date_id"].unique()[0])
    
    logging.info("prepare final data")
    trafic = trafic[["date_id", "id_site", "trafic_data_go", "trafic_voix_erl", "techno"]]
    trafic = trafic.groupby(["date_id", "id_site", "techno"]).sum().unstack()
    trafic.columns = ["_".join(d) for d in trafic.columns]
    trafic.reset_index(drop=False, inplace=True)
    trafic.columns = ["jour", "id_site", "trafic_data_2g", "trafic_data_3g", "trafic_data_4g", "trafic_voix_2g", "trafic_voix_3g", "trafic_voix_4g"]
    
    # Save the cleaned DataFrame to Minio
    trafic["trafic_voix_total"] = trafic["trafic_voix_2g"] + trafic["trafic_voix_3g"] + trafic["trafic_voix_4g"]
    trafic["trafic_data_total"] = trafic["trafic_data_2g"] + trafic["trafic_data_3g"] + trafic["trafic_data_4g"]
    trafic["id_site"] = trafic["id_site"].astype("float")
    return trafic

def cleaning_congestion(client, date: str):
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
    exec_date = datetime.strptime(date, CONFIG["date_format"])
     # Find the required object in the CONFIG dictionary
    objet = next((table for table in CONFIG["tables"] if table["name"] == "ks_hebdo_tdb_radio_drsi"), None)
    if not objet:
        raise ValueError("Table ks_hebdo_tdb_radio_drsi not found.")
     # Check if bucket exists
    if not client.bucket_exists(objet["bucket"]):
        raise ValueError(f"Bucket {objet['bucket']} does not exist.")
     # Split the date into parts
    date_parts = date.split("-")
    filename = get_latest_file(client, objet["bucket"], prefix = f"{objet['folder']}/{date_parts[0]}/{date_parts[1]}/{date_parts[2]}")
    if filename is not None:
        df_ = read_file(client=client, bucket_name=objet['bucket'], object_name=filename, sep=',')
    df_.columns = df_.columns.str.lower()
    df_["date_id"] = exec_date
    df_ = df_.loc[:,["date_id", "id_site", "nbre_cellule", "nbre_cellule_congestionne", "techno"]]
    df_.columns = ["jour", "id_site", "cellules", "cellules_congestionnees", "techno"]
    df_ = df_.loc[df_.techno != "TDD", :]
    df_ = df_.groupby(["jour",	"id_site","techno"	]).sum()
    df_ = df_.unstack()
    df_.columns = ["_".join(d) for d in df_.columns]
    df_ = df_.reset_index(drop=False)
    df_.columns = ["jour", "id_site", "cellules_2g", "cellules_3g", "cellules_4g", "cellules_2g_congestionnees", "cellules_3g_congestionnees", "cellules_4g_congestionnees"]
    df_["cellules_totales"] = df_["cellules_2g"] + df_["cellules_3g"] + df_["cellules_4g"]
    df_["cellules_congestionnees_totales"] = df_["cellules_2g_congestionnees"] + df_["cellules_3g_congestionnees"] + df_["cellules_4g_congestionnees"]
    df_["id_site"] = df_["id_site"].astype("float")
    return df_