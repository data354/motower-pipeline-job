from datetime import datetime
from airflow.models import Variable
from pathlib import Path
import json
from airflow import DAG
from airflow.operators.python import PythonOperator
from gps.common.extract import extract_ftp
from gps.common.rwminio import save_minio

FTP_HOST = Variable.get('ftp_host')
FTP_USER = Variable.get('ftp_user')
FTP_PASSWORD = Variable.get('ftp_password')

MINIO_ENDPOINT = Variable.get('minio_host')
MINIO_ACCESS_KEY = Variable.get('minio_access_key')
MINIO_SECRET_KEY = Variable.get('minio_secret_key')

INGEST_DATE = "{{ macros.ds_add(ds, -1) }}"


config_file = Path(__file__).parents[3] / "config/configs.json"
if config_file.exists():
    with config_file.open("r",) as f:
        config = json.load(f)
else:
    raise RuntimeError("configs file don't exists")

def extract_ftp_job(**kwargs):
    """
    """

    data = extract_ftp(FTP_HOST,FTP_USER, FTP_PASSWORD )
    if data.shape[0] != 0:
        save_minio(MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, kwargs["bucket"], kwargs["folder"] , kwargs["ingest_date"], data)
    else:
        raise RuntimeError(f"No data for {kwargs['ingest_date']}")


with DAG(
    'etl',
    default_args={
        'depends_on_past': False,
        'email': ["yasmine.kouadio@data354.co"],
        'email_on_failure': True,
        'email_on_retry': False,
        'max_active_run': 1,
        'retries': 0
    },
    description='ingest data from ftp',
    schedule_interval="30 5 * * *",
    start_date=datetime(2022, 9, 2, 5, 30, 0),
    catchup=True
) as dag:
    
    ingest_ftp = PythonOperator(
        task_id='ingest_ftp',
        provide_context=True,
        python_callable=extract_ftp_job,
        op_kwargs={'bucket': config["tables"][7]["bucket"],
                   'folder': config["tables"][7]["folder"],
                   'ingest_date': INGEST_DATE},
        dag=dag,
    ),

    ingest_ftp
