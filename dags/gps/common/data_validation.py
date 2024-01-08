import logging
from typing import List
import pandas as pd
from gps.common.alerting import send_email
from gps import CONFIG

def validate_column(df, file_type: str, col:str, host: str, port: int, user: str, receivers: List = CONFIG["default_adress"], date = None): # just for monthly
    """
    Validate data
    """
    logging.info("START VALIDATION OF COLUMN %s", col)
    thresholds = CONFIG["threshold"][col]
    df_not_valid = df[not((df[col] >= thresholds["min"]) & (df[col] <= thresholds["max"]))]
    date_for_group = "date_id" if file_type.upper() == "TRAFIC" else "day_id"
    if not df_not_valid.empty:
        message = f"These dates have invalid {col}: {df_not_valid[[date_for_group, col]].to_string(index=False)}" if date is None else f"FILE OF {date} have invalid {col}: {df_not_valid[[date_for_group, col]].to_string(index=False)}"
        send_email( host= host, port = port, user= user, receivers= receivers, subject= f"VALIDATION ERRORS,ON FILE {file_type.upper()}", content=message)
        raise ValueError(message)
    logging.info("DATA OF COLUMN %s ARE VALID", col)



def validate_site_actifs(df_bdd_site, col: str, host: str, port: int, user: str, receivers: List = CONFIG["default_adress"]):
    """
        validate number of actifs sites from bdd site
    """
    logging.info("START VALIDATION ")
    thresholds = CONFIG["thresold"]["sites_actifs"]
    number = df_bdd_site[col].nunique()
    try:
        mois = df_bdd_site["mois"].unique()[0]
    except KeyError:
        mois = df_bdd_site["jour"].unique()[0]
  
    if not (thresholds["min"] <= number <= thresholds["max"]):
        message = f"BDD SITE FILE OF MONTH {mois} HAS INVALID NUMBER OF ACTIFS SITES {number}"
        send_email(host=host, port=port, user=user, receivers=receivers, subject="VALIDATION ERROR FOR FILE BASE DE DONNEES DES SITE", content=message)
        raise ValueError(message)
    logging.info("VALID ACTIFS SITES %s", str(number))


