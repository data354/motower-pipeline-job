# from datetime import timedelta, datetime
# from airflow import DAG
# from airflow.utils.task_group import TaskGroup
# from airflow.operators.python import PythonOperator
# from airflow.sensors.python import PythonSensor
# from airflow.models import Variable
# from minio import Minio
# from gps import CONFIG
# from gps.common.extract import extract_pg
# from gps.common.rwminio import save_minio

# PG_HOST = Variable.get('pg_host')
# PG_V2_DB = Variable.get('pg_v2_db')
# PG_V2_USER = Variable.get('pg_v2_user')
# PG_V2_PASSWORD = Variable.get('pg_v2_password')

# MINIO_ENDPOINT = Variable.get('minio_host')
# MINIO_ACCESS_KEY = Variable.get('minio_access_key')
# MINIO_SECRET_KEY = Variable.get('minio_secret_key')

# CLIENT = Minio( MINIO_ENDPOINT,
#         access_key= MINIO_ACCESS_KEY,
#         secret_key= MINIO_SECRET_KEY,
#         secure=False)

# DATE = "{{ds}}"

# def extract_congestion(**kwargs):
#     """
#     """
#     data = extract_pg(host = PG_HOST, database= PG_V2_DB, user= PG_V2_USER, 
#             password= PG_V2_PASSWORD , table= kwargs["thetable"] , date= kwargs["ingest_date"])
#     print(data.shape)
#     if  data.empty:
#         raise RuntimeError(f"No data for {kwargs['ingest_date']}")
    
#     save_minio(CLIENT, kwargs["bucket"] , kwargs["ingest_date"], data, kwargs["folder"])


# with DAG(
#     "enrich",
#     default_args={
#         "depends_on_past": False,
#         "email": CONFIG["airflow_receivers"],
#         "email_on_failure": True,
#         "email_on_retry": False,
#         "max_active_run": 1,
#         "retries": 0,
#     },
#     description="clean monthly data",
#     schedule_interval="0 0 6 * *",
#     start_date=datetime(2023, 1, 6, 0, 0, 0),
#     catchup=True,
# ) as dag:
    