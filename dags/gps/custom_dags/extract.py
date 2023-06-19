"""  EXTRACT DAG"""

from datetime import datetime
import sqlalchemy

from minio import Minio
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow import DAG
from gps import CONFIG

from gps.common.extract import extract_pg, extract_ftp
from gps.common.rwminio import save_minio


PG_HOST = Variable.get('pg_host')
PG_DB = Variable.get('pg_db')
PG_USER = Variable.get('pg_user')
PG_PASSWORD = Variable.get('pg_password')

MINIO_ENDPOINT = Variable.get('minio_host')
MINIO_ACCESS_KEY = Variable.get('minio_access_key')
MINIO_SECRET_KEY = Variable.get('minio_secret_key')

FTP_HOST = Variable.get('ftp_host')
FTP_USER = Variable.get('ftp_user')
FTP_PASSWORD = Variable.get('ftp_password')

PG_SAVE_HOST = Variable.get('pg_save_host')
PG_SAVE_DB = Variable.get('pg_save_db')
PG_SAVE_USER = Variable.get('pg_save_user')
PG_SAVE_PASSWORD = Variable.get('pg_save_password')

INGEST_PG_DATE = "{{ macros.ds_add(ds, -1) }}"
INGEST_FTP_DATE = "{{ macros.ds_add(ds, -6) }}"



CLIENT = Minio( MINIO_ENDPOINT,
        access_key= MINIO_ACCESS_KEY,
        secret_key= MINIO_SECRET_KEY,
        secure=False)

def extract_job(**kwargs):
    """
        extract callable
    """
    ingest_date = datetime.strptime(kwargs["ingest_date"], CONFIG["date_format"])
    thetable = kwargs["thetable"]
    if ingest_date < datetime(2022,12,30) :
        if thetable in ["faitalarme"]:
            data = extract_pg(host = PG_HOST, database= PG_DB, user= PG_USER,
                   password= PG_PASSWORD , table= thetable , date= kwargs["ingest_date"])
            if not data.empty:
                save_minio(CLIENT, kwargs["bucket"],
                        kwargs["folder"] , kwargs["ingest_date"], data)
            else:
                raise RuntimeError(f"No data for {kwargs['ingest_date']}")
    elif ( ingest_date > datetime(2022,12,29) ) and ( ingest_date <= datetime(2023,2,28) ):
        if thetable in ["faitalarme", "hourly_datas_radio_prod"]:
            if thetable == "hourly_datas_radio_prod":
                thetable = "hourly_datas_radio_prod_archive"
            data = extract_pg(host = PG_HOST, database= PG_DB, user= PG_USER,
                    password= PG_PASSWORD , table= thetable , date= kwargs["ingest_date"])

            if not data.empty:
                save_minio(CLIENT, kwargs["bucket"], kwargs["folder"] , kwargs["ingest_date"], data)
            else:
                raise RuntimeError(f"No data for {kwargs['ingest_date']}")
    elif ( ingest_date >= datetime(2023,3,1) ) and ( ingest_date < datetime(2023,3,3) ):
        if thetable in ["faitalarme", "hourly_datas_radio_prod"]:
            data = extract_pg(host = PG_HOST, database= PG_DB, user= PG_USER,
                    password= PG_PASSWORD , table= thetable , date= kwargs["ingest_date"])

            if not data.empty:
                save_minio(CLIENT, kwargs["bucket"], kwargs["folder"] , kwargs["ingest_date"], data)
            else:
                raise RuntimeError(f"No data for {kwargs['ingest_date']}")
    else:
        data = extract_pg(host = PG_HOST, database= PG_DB, user= PG_USER,
            password= PG_PASSWORD , table= thetable , date= kwargs["ingest_date"])
        if not data.empty:
            save_minio(CLIENT, kwargs["bucket"], kwargs["folder"] , kwargs["ingest_date"], data)
        else:
            raise RuntimeError(f"No data for {kwargs['ingest_date']}")

def extract_ftp_job(**kwargs):
    """
    extract ftp files callable
 
    """
    print(sqlalchemy.__version__)
    ingest_date = datetime.strptime(kwargs["ingest_date"], CONFIG["date_format"])
    if ingest_date >= datetime(2022, 9, 1):
        data = extract_ftp(FTP_HOST, FTP_USER, FTP_PASSWORD, kwargs["ingest_date"])
        if not data.empty:
            save_minio(CLIENT, kwargs["bucket"], kwargs["folder"], kwargs["ingest_date"], data)
        else:
            raise RuntimeError(f"No data for {kwargs['ingest_date']}")
with DAG(
        'extract',
        default_args={
            'depends_on_past': False,
            'wait_for_downstream': False,
            'email': CONFIG["airflow_receivers"],
            'email_on_failure': True,
            'email_on_retry': False,
            'max_active_run': 1,
            'retries': 0
        },
        description='ingest data',
        schedule_interval="30 5 * * *",
        start_date=datetime(2022, 9, 1, 6, 30, 0),
        catchup=True
) as dag:

    tasks = []
    for table_config in CONFIG["tables"]:
        task_id = f'ingest_{table_config["name"]}'
        callable_fn = extract_job if table_config["name"] != "ca&parc" else extract_ftp_job
        INGEST_DATE = INGEST_PG_DATE if table_config["name"] != "ca&parc" else INGEST_FTP_DATE
        task = PythonOperator(
            task_id=task_id,
            provide_context=True,
            python_callable=callable_fn,
            op_kwargs={
                'thetable': table_config["name"],
                'bucket': table_config["bucket"],
                'folder': table_config["folder"],
                'table': table_config["table"],
                'ingest_date': INGEST_DATE
            },
            dag=dag,
        )
        tasks.append(task)
    tasks

if __name__ == "__main__":
    from airflow.utils.state import State

    dag.clear()
    dag.run()
