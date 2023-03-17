from pathlib import Path
from datetime import datetime
import yaml
from airflow import DAG
from gps.common.extract import extract, save_minio
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.macros import ds_add


PG_HOST = Variable.get('pg_host')
PG_DB = Variable.get('pg_db')
PG_USER = Variable.get('pg_user')
PG_PASSWORD = Variable.get('pg_password')

MINIO_ENDPOINT = Variable.get('minio_host')
MINIO_ACCESS_KEY = Variable.get('minio_access_key')
MINIO_SECRET_KEY = Variable.get('minio_secret_key')

INGEST_DATE = "{{ macros.ds_add(ds, -2) }}"


config_file = Path(__file__).parents[3] / "config/configs.yaml"
if config_file.exists():
    with config_file.open("r",) as f:
        config = yaml.safe_load(f)
else:
    raise RuntimeError("configs file don't exists")



def extract_job(**kwargs):
    """
        extract
    """


    data = extract(PG_HOST, PG_DB, PG_USER, PG_PASSWORD , kwargs["thetable"] , kwargs["ingest_date"])

    if data.shape[0] == 0:
        save_minio(MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, kwargs["thetable"], kwargs["ingest_date"], data)
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
    description='ingest data from postgresql',
    schedule_interval="30 5 * * *",
    start_date=datetime(2023, 2, 3, 5, 30, 0),
    catchup=True
) as dag:

    ingest_hdrp = PythonOperator(
        task_id='ingest_hourly_datas_radio_prod',
        provide_context=True,
        python_callable=extract_job,
        op_kwargs={'thetable': config["tables"][0],
                   'ingest_date': INGEST_DATE},
        dag=dag,
    ),

    ingest_ts2g = PythonOperator(
        task_id='ingest_Taux_succes_deuxg',
        provide_context=True,
        python_callable=extract_job,
        op_kwargs={'thetable': config["tables"][1],
                   'ingest_date': INGEST_DATE},
        dag=dag,
    ),

    ingest_ts3g = PythonOperator(
        task_id='ingest_Taux_succes_troisg',
        provide_context=True,
        python_callable=extract_job,
        op_kwargs={'thetable': config["tables"][2],
                   'ingest_date': INGEST_DATE},
        dag=dag,
    ),
    ingest_cd2g = PythonOperator(
        task_id='ingest_call_drop_deuxg',
        provide_context=True,
        python_callable=extract_job,
        op_kwargs={'thetable': config["tables"][3],
                   'ingest_date': INGEST_DATE},
        dag=dag,
    ),
    ingest_cd3g = PythonOperator(
        task_id='ingest_call_drop_troisg',
        provide_context=True,
        python_callable=extract_job,
        op_kwargs={'thetable': config["tables"][4],
                   'ingest_date': INGEST_DATE},
        dag=dag,
    )

    [ingest_hdrp, ingest_ts2g, ingest_ts3g, ingest_cd2g, ingest_cd3g]

if __name__ == "__main__":
    from airflow.utils.state import State

    dag.clear()
    dag.run()
