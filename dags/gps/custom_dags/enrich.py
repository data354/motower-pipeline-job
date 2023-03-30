from datetime import datetime
from airflow import DAG
from gps.common.enrich import cleaning_base_site
from airflow.operators.python import PythonOperator
from airflow.models import Variable


MINIO_ENDPOINT = Variable.get('minio_host')
MINIO_ACCESS_KEY = Variable.get('minio_access_key')
MINIO_SECRET_KEY = Variable.get('minio_secret_key')

DATE = "{{data_interval_start.strftime('%Y-%m-%d')}}"


with DAG(
    'enrich',
    default_args={
        'depends_on_past': False,
        'email': ["yasmine.kouadio@data354.co"],
        'email_on_failure': True,
        'email_on_retry': False,
        'max_active_run': 1,
        'retries': 0
    },
    description='clean and enrich monthly data',
    schedule_interval= "@monthly",
    start_date=datetime(2023, 1, 6, 6, 30, 0),
    catchup=True
) as dag:

    clean_base_site = PythonOperator(
        task_id='enrich_base_sites',
        provide_context=True,
        python_callable=cleaning_base_site,
        op_kwargs={'endpoint': MINIO_ENDPOINT,
                   'access_key': MINIO_ACCESS_KEY,
                   'secret_key': MINIO_SECRET_KEY,
                   'date': DATE},
        dag=dag
    )
    clean_base_site
