from datetime import timedelta, datetime
from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor
from airflow.models import Variable
from minio import Minio
from gps import CONFIG
from gps.common.extract import extract_pg
from gps.common.rwminio import save_minio
from gps.common.weekly import cleaning_congestion
from gps.common.rwpg import write_pg


PG_HOST = Variable.get('pg_host')
PG_V2_DB = Variable.get('pg_v2_db')
PG_V2_USER = Variable.get('pg_v2_user')
PG_V2_PASSWORD = Variable.get('pg_v2_password')

MINIO_ENDPOINT = Variable.get('minio_host')
MINIO_ACCESS_KEY = Variable.get('minio_access_key')
MINIO_SECRET_KEY = Variable.get('minio_secret_key')

PG_SAVE_HOST = Variable.get('pg_save_host')
PG_SAVE_DB = Variable.get('pg_save_db')
PG_SAVE_USER = Variable.get('pg_save_user')
PG_SAVE_PASSWORD = Variable.get('pg_save_password')

CLIENT = Minio( MINIO_ENDPOINT,
        access_key= MINIO_ACCESS_KEY,
        secret_key= MINIO_SECRET_KEY,
        secure=False)

DATE = "{{ds}}"

def extract_v2(**kwargs):
    """
    """
    data = extract_pg(host = PG_HOST, database= PG_V2_DB, user= PG_V2_USER, 
            password= PG_V2_PASSWORD , table= kwargs["thetable"] , date= kwargs["ingest_date"])
    if  data.empty:
        raise RuntimeError(f"No data for {kwargs['ingest_date']}")
    
    save_minio(CLIENT, kwargs["bucket"] , kwargs["ingest_date"], data, kwargs["folder"])

def gen_congestion(**kwargs):
    """
    """
    data = cleaning_congestion(CLIENT, MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, kwargs["date"])
    if  data.empty:
        raise RuntimeError(f"No data for {kwargs['ingest_date']}")
    write_pg(PG_SAVE_HOST, PG_SAVE_DB, PG_SAVE_USER, PG_SAVE_PASSWORD, data, "congestion")


with DAG(
    "weekly",
    default_args={
        "depends_on_past": False,
        "email": CONFIG["airflow_receivers"],
        "email_on_failure": True,
        "email_on_retry": False,
        "max_active_run": 1,
        "retries": 0,
    },
    description="weekly data",
    schedule_interval=timedelta(weeks=1),
    start_date=datetime(2023, 7, 3, 20, 0, 0),
    catchup=True,
) as dag:
    table_config = next((table for table in CONFIG["tables"] if table["name"] == "ks_hebdo_tdb_radio_drsi"), None)
    extract_congestion_task = PythonOperator(
                task_id="extract_congestion",
                provide_context=True,
                python_callable=extract_v2,
                op_kwargs={
                    'thetable': table_config["name"],
                    'bucket': table_config["bucket"],
                    'folder': table_config["folder"],
                    'table': table_config["table"],
                    'ingest_date': DATE
                },
                dag=dag,
            )
    gen_congestion_task = PythonOperator(
            task_id="gen_congestion_task",
            provide_context=True,
            python_callable=gen_congestion,
            op_kwargs={
                "client": CLIENT,
                "endpoint": MINIO_ENDPOINT,
                "accesskey": MINIO_ACCESS_KEY,
                "secretkey": MINIO_SECRET_KEY,
                "date": DATE,
            },
            #on_failure_callback=on_failure,
            dag=dag,
        )
    extract_congestion_task >> gen_congestion_task