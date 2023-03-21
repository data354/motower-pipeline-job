from pathlib import Path
from datetime import datetime
import json
from airflow import DAG
from minio import Minio
from gps.common.extract import extract, save_minio
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from io import BytesIO


PG_HOST = Variable.get('pg_host')
PG_DB = Variable.get('pg_db')
PG_USER = Variable.get('pg_user')
PG_PASSWORD = Variable.get('pg_password')

MINIO_ENDPOINT = Variable.get('minio_host')
MINIO_ACCESS_KEY = Variable.get('minio_access_key')
MINIO_SECRET_KEY = Variable.get('minio_secret_key')


def extract_fa():
    data = extract(host= PG_HOST, database= PG_DB, user= PG_USER, password= PG_PASSWORD , table=None, date=None , request="select date, code_site, techno, delay , nbrecellule from faitalarme where date between '2023-01-01' and '2023-03-21' order by date;")
    client = Minio(
        MINIO_ENDPOINT,
        access_key= MINIO_ACCESS_KEY,
        secret_key= MINIO_SECRET_KEY,
        secure=False)
    
    csv_bytes = data.to_csv().encode('utf-8')
    csv_buffer = BytesIO(csv_bytes)
    client.put_object("trafic",
                       "faitalarme-01-03-2023.csv",
                        data=csv_buffer,
                        length=len(csv_bytes),
                        content_type='application/csv')
    


with DAG(
    'etl_archive',
    default_args={
        'depends_on_past': False,
        'email': ["yasmine.kouadio@data354.co"],
        'email_on_failure': True,
        'email_on_retry': False,
        'max_active_run': 1,
        'retries': 0
    },
    description='ingest data from postgresql',
    schedule_interval= "None",
    start_date=datetime(2023, 1, 3, 5, 30, 0),
    catchup=True
) as dag:

    ingest_fa = PythonOperator(
        task_id='ingest_hourly_datas_radio_prod_archive',
        provide_context=True,
        python_callable=extract_fa,
        dag=dag
    )

    ingest_fa