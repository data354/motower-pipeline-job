from pathlib import Path
import logging
from datetime import datetime, timedelta
import json
from minio import Minio
import pandas as pd
import psycopg2
from io import BytesIO


# Get BD settings

#db_file = Path(__file__).parents[3] / "config/database.yaml"
config_file = Path(__file__).parents[3] / "config/configs.json"
# if db_file.exists():
#     with db_file.open("r",) as f:
#         settings = yaml.safe_load(f)
# else:
#     raise RuntimeError("database file don't exists")

if config_file.exists():
    with config_file.open("r",) as f:
        config = json.load(f)
else:
    raise RuntimeError("configs file don't exists")


# init spark session
# spark = SparkSession.builder.appName("Extract_data_from_pg").getOrCreate()

# def extract_spark(table: str, day: str)-> None:
#     """
#         function to get data from table and save in parquet file
#         Args:
#             table [str]: table name
#         Return:
#             None

#     """
#     # get table
#     try:
#         df = spark.read.format("jdbc")\
#             .option("url", f"jdbc:postgresql://{settings['DATABASE']['HOST']}:{settings['DATABASE']['PORT']}/{settings['DATABASE']['DB']}") \
#             .option("driver", "org.postgresql.Driver").option("dbtable", table) \
#             .option("user", f"{settings['DATABASE']['USERNAME']}").option("password", f"{settings['DATABASE']['PASSWORD']}").load()
#         df.createOrReplaceTempView("data")
#     except Exception as ex:
#         print(ex)

#     data_of_day = spark.sql(f"select * from data where date_jour = {day}")

#     # save table
#     client = Minio(
#         settings['MINIO']['endpoint'],
#         access_key=settings['MINIO']['s3accessKeyAws'],
#         secret_key=settings['MINIO']['s3secretKeyAws'],
#         secure=False
#     )
#     if not client.bucket_exists("table"):
#       client.make_bucket("table")
#     outputpath = f"s3a://{table}/{table}-{day}.parquet"
#     data_of_day.write.mode("overwrite").parquet(outputpath)


def extract(host: str, database:str, user: str, password: str, table: str = None,  date: str = None, request:str = None) -> pd.DataFrame:
    """
        function to get data from table and save in parquet file
           Args: 
             table [str]: table name
             date: str
         Return:
             pandas DataFrame
    """
        # connect to the PostgreSQL server
              

        # execute a statement
    logging.info("Getting data of the table %s ", table)
    logging.info("where date is %s", date)
    if table == "hourly_datas_radio_prod":
        sql = f"""select  date_jour, code_site, sum(trafic_voix) as trafic_voix, sum(trafic_data) as trafic_data, techno from 
                    {table} where date_jour = '{date.replace("-","")}' group by date_jour, code_site, techno;"""
    elif table == "hourly_datas_radio_prod_archive":
        sql = f"""select  date_jour, code_site, sum(trafic_voix) as trafic_voix, sum(trafic_data) as trafic_data, techno from 
                    {table} where date_jour = '{date.replace("-","")}' group by date_jour, code_site, techno;"""
    elif table == "Taux_succes_2g":
        sql = f"""select date_jour, SPLIT_PART(bcf_name,'_',1) AS code_site, MIN(CAST(cssr_cs AS DECIMAL)) AS min_cssr_cs,
                MAX(CAST(cssr_cs AS DECIMAL)) AS max_cssr_cs, AVG(CAST(cssr_cs AS DECIMAL)) AS avg_cssr_cs,
                median(CAST(cssr_cs AS DECIMAL)) AS median_cssr_cs, techno 
                from {table} where date_jour='{date.replace("-","")}' group by date_jour, code_site, techno;"""
    elif table == "Call_drop_2g":
        sql = f"""select date_jour, SPLIT_PART(bcf_name,'_',1) AS code_site,SUM(CAST(drop_after_tch_assign AS INTEGER)) as drop_after_tch_assign, techno 
            from {table} where date_jour='{date.replace("-","")}' group by date_jour, code_site, techno;"""
    elif table == "Call_drop_3g":
        sql = f"""select date_jour, SPLIT_PART(wbts_name,'_',1) AS code_site,SUM(CAST(number_of_call_drop_3g AS INTEGER)) as number_of_call_drop_3g, techno 
            from {table} where date_jour='{date.replace("-","")}' group by date_jour, code_site, techno;"""
    elif table == "Taux_succes_3g":
        sql = f'''select date_jour, SPLIT_PART(wbts_name,'_',1) AS code_site, MIN(CAST("3g_call_setup_suceess_rate_speech_h" AS DECIMAL)) as min_cssr_cs,
                  MAX(CAST("3g_call_setup_suceess_rate_speech_h" AS DECIMAL)) as max_cssr_cs, 
                 AVG(CAST("3g_call_setup_suceess_rate_speech_h" AS DECIMAL)) as avg_cssr_cs, median(CAST("3g_call_setup_suceess_rate_speech_h" AS DECIMAL)) 
                 as median_cssr_cs , techno
                 from {table} where date_jour='{date.replace("-","")}' group by date_jour, code_site, techno;'''
    elif (table is None) and (date is None) and (request is not None)  :
        sql = request
    else:
        raise RuntimeError(f"No request for this table {table}")
    logging.info('Connecting to the PostgreSQL database...')
    try:
        agregate_data = pd.DataFrame()
        with psycopg2.connect(host= host,database= database,user= user,password= password,) as conn:
            agregate_data = pd.read_sql_query(sql, conn)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    return agregate_data


def save_minio(endpoint, accesskey, secretkey, table: str, date: str, data: pd.DataFrame) -> None:
    """
        save dataframe in minio
        Args:
            table [str]
            date [str]
            df [pd.DataFrame]
        Return
            None
    """
    client = Minio(
        endpoint,
        access_key= accesskey,
        secret_key= secretkey,
        secure=False)
    logging.info("start to save data")
    objet = [t for t in config["tables"] if t["name"] == table][0]
    if not client.bucket_exists(objet.get("bucket")):
        client.make_bucket(objet.get("bucket"))
    csv_bytes = data.to_csv().encode('utf-8')
    csv_buffer = BytesIO(csv_bytes)
    client.put_object(objet.get("bucket"),
                       f"{objet.get('folder')}/{date.split('-')[0]}/{date.split('-')[1]}/{date.split('-')[2]}.csv",
                        data=csv_buffer,
                        length=len(csv_bytes),
                        content_type='application/csv')

