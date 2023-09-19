import pandas as pd
from gps import CONFIG
import logging

def validate_column(df, col:str, date = None): # just for monthly
    """
        validate data 
    """
    logging.info("START VALIDATION OF COLUMN %s", col)
    thresolds = CONFIG["thresold"][col]
    df_not_valid = df[~((df[col] >= thresolds["min"]) & (df[col] <= thresolds["max"]) )]
    if df_not_valid.shape[0]:
        message = f"These dates have invalid {col}: {df_not_valid.loc[:, ['day_id', col]].to_string()}" if date is None else f"FILE OF {date} have invalid {col}: {df_not_valid.loc[:, ['day_id', col]].to_string()}"
        raise ValueError(message)
    logging.info("DATA OF COLUMN %s ARE VALID", col)

def validate_site_actifs(df_bdd_site, col):
    """
        validate number of actifs sites from bdd site
    """
    logging.info("START VALIDATION ")
    thresolds = CONFIG["thresold"]["sites_actifs"]
    number = len(df_bdd_site[col].unique())
    try:
        mois = df_bdd_site["mois"].unique()[0]
    except KeyError:
        mois = df_bdd_site["jour"].unique()[0]
  
    if not((number >= thresolds["min"]) & (number <= thresolds["max"])):
        message = f"BDD SITE FILE OF MONTH {mois} HAVE INVALID NUMBER OF ACTIFS SITES {number}"
        raise ValueError(message)
    logging.info(f"VALID ACTIFS SITES {number}")
