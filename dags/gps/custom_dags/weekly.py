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
from gps.common.weekly import motower_weekly
from gps.common.alerting import alert_failure
from gps.common.weekly import cleaning_daily_trafic



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

SMTP_HOST = Variable.get('smtp_host')
SMTP_PORT = Variable.get('smtp_port')
SMTP_USER = Variable.get('smtp_user')

CLIENT = Minio( MINIO_ENDPOINT,
        access_key= MINIO_ACCESS_KEY,
        secret_key= MINIO_SECRET_KEY,
        secure=False)

DATE = "{{data_interval_start | ds}}"

def on_failure(context):
    """
    Function to handle task failure
    """
    params = {
        "host": SMTP_HOST,
        "port": SMTP_PORT,
        "user": SMTP_USER,
        "task_id": context["task"].task_id,
        "dag_id": context["task"].dag_id,
        "exec_date": context.get("ts"),
        "exception": context.get("exception"),
    }
    # if "cleaning_bdd" in params["task_id"]:
    #     params["type_fichier"] = "BASE_SITES"
    # elif "cleaning_esco" in params["task_id"]:
    #     params["type_fichier"] = "OPEX_ESCO"
    # elif "cleaning_ihs" in params["task_id"]:
    #     params["type_fichier"] = "OPEX_IHS"
    # else:
    #     raise RuntimeError("Can't get file type")
    alert_failure(**params)

def extract_v2(**kwargs):
    """
    """
    data = extract_pg(host = PG_HOST, database= PG_V2_DB, user= PG_V2_USER, 
            password= PG_V2_PASSWORD , table= kwargs["thetable"] , date= kwargs['ingest_date'])
    if  data.empty:
        raise RuntimeError(f"No data for {kwargs['ingest_date']}")
    
    save_minio(CLIENT, kwargs["bucket"] , kwargs['ingest_date'], data, kwargs["folder"])

def clean_trafic(**kwargs):
    """
    """
    data = cleaning_daily_trafic(CLIENT, MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, kwargs['ingest_date'])
    if  data.empty:
        raise RuntimeError(f"No data for {kwargs['ingest_date']}")
    save_minio(CLIENT, kwargs["bucket"], kwargs['ingest_date'], data, f'{kwargs["folder"]}-cleaned')

def gen_congestion(**kwargs):
    """
    """
    data = cleaning_congestion(CLIENT, MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, kwargs['ingest_date'])
    if  data.empty:
        raise RuntimeError(f"No data for {kwargs['date']}")
    save_minio(CLIENT, kwargs["bucket"] , kwargs['date'], data, kwargs["folder"]+"-cleaned")

def gen_motower_weekly(**kwargs):
    """
    """
    data = motower_weekly(CLIENT, MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, kwargs['date'], PG_SAVE_HOST, PG_SAVE_USER, PG_SAVE_PASSWORD, PG_SAVE_DB )
    if  data.empty:
        raise RuntimeError(f"No data for {kwargs['date']}")
    write_pg(PG_SAVE_HOST, PG_SAVE_DB, PG_SAVE_USER, PG_SAVE_PASSWORD, data, "motower_weekly")
with DAG(
    "weekly",
    default_args={
        'depends_on_past': True,
        'wait_for_downstream': False,
        "email": CONFIG["airflow_receivers"],
        "email_on_failure": True,
        "email_on_retry": False,
        "max_active_run": 1,
        "retries": 0,
    },
    description="weekly data",
    schedule_interval=timedelta(weeks=1),
    start_date=datetime(2023, 6, 26, 12, 0, 0),
    catchup=True,
) as dag:
    table_config = next((table for table in CONFIG["tables"] if table["name"] == "ks_daily_tdb_radio_drsi"), None)
    extract_trafic = PythonOperator(
                task_id="extract_trafic",
                provide_context=True,
                python_callable=extract_v2,
                on_failure_callback=on_failure,
                op_kwargs={
                    'thetable': table_config["name"],
                    'bucket': table_config["bucket"],
                    'folder': table_config["folder"],
                    'table': table_config["table"],
                    'ingest_date': DATE
                },
                dag=dag,
            )
    clean_trafic_task = PythonOperator(
            task_id="clean_trafic_task",
            provide_context=True,
            python_callable=clean_trafic,
            on_failure_callback=on_failure,
            op_kwargs={
                "client": CLIENT,
                'bucket': table_config["bucket"],
                'folder': table_config["folder"],
                'table': table_config["table"],
                "ingest_date": DATE,
            },
            # on_failure_callback=on_failure,
            dag=dag,
        )
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
                'thetable': table_config["name"],
                'bucket': table_config["bucket"],
                'folder': table_config["folder"],
                'table': table_config["table"],
                'ingest_date': DATE
            },
            #on_failure_callback=on_failure,
            dag=dag,
        )
    gen_motower_task =  PythonOperator(
            task_id="gen_motower_task",
            provide_context=True,
            python_callable=gen_motower_weekly,
            op_kwargs={
                
                "date": DATE
            },
            #on_failure_callback=on_failure,
            dag=dag,
        )
    extract_congestion_task >> gen_congestion_task >> gen_motower_task  


    if __name__ == "__main__":
        from airflow.utils.state import State

        dag.clear()
        dag.run()