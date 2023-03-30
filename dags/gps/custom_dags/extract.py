from pathlib import Path
from datetime import datetime
from airflow import DAG
from gps.common.extract import extract_pg, extract_ftp
from gps.common.rwminio import save_minio
from gps import CONFIG
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

FTP_HOST = Variable.get('ftp_host')
FTP_USER = Variable.get('ftp_user')
FTP_PASSWORD = Variable.get('ftp_password')

INGEST_PG_DATE = "{{ macros.ds_add(ds, -1) }}"
INGEST_FTP_DATE = "{{ macros.ds_add(ds, -6) }}"





def extract_job(**kwargs):
    """
        extract
    """


    data = extract_pg(PG_HOST, PG_DB, PG_USER, PG_PASSWORD , kwargs["thetable"] , kwargs["ingest_date"])

    if data.shape[0] != 0:
        save_minio(MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, kwargs["bucket"], 
                   kwargs["folder"] , kwargs["ingest_date"], data)
    else:
        raise RuntimeError(f"No data for {kwargs['ingest_date']}")

def extract_ftp_job(**kwargs):
    """
    extract ftp files

    """

    data = extract_ftp(FTP_HOST,FTP_USER, FTP_PASSWORD , kwargs["ingest_date"])
    if data.shape[0] != 0:
        save_minio(MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, kwargs["bucket"], 
                   kwargs["folder"] , kwargs["ingest_date"], data)
    else:
        raise RuntimeError(f"No data for {kwargs['ingest_date']}")

with DAG(
    'extract',
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
    start_date=datetime(2023, 2, 7, 6, 30, 0),
    catchup=True
) as dag:

    ingest_hdrp = PythonOperator(
        task_id='ingest_hourly_datas_radio_prod',
        provide_context=True,
        python_callable=extract_job,
        op_kwargs={'thetable': CONFIG["tables"][0]["name"],
                   'bucket': CONFIG["tables"][0]["bucket"],
                   'folder': CONFIG["tables"][0]["folder"],
                   'ingest_date': INGEST_PG_DATE},
        dag=dag,
    ),

    ingest_ts2g = PythonOperator(
        task_id='ingest_Taux_succes_deuxg',
        provide_context=True,
        python_callable=extract_job,
        op_kwargs={'thetable': CONFIG["tables"][2]["name"],
                   'bucket': CONFIG["tables"][2]["bucket"],
                   'folder': CONFIG["tables"][0]["folder"],
                   'ingest_date': INGEST_PG_DATE},
        dag=dag,
    ),

    ingest_ts3g = PythonOperator(
        task_id='ingest_Taux_succes_troisg',
        provide_context=True,
        python_callable=extract_job,
        op_kwargs={'thetable': CONFIG["tables"][3]["name"],
                   'bucket': CONFIG["tables"][3]["bucket"],
                   'folder': CONFIG["tables"][3]["folder"],
                   'ingest_date': INGEST_PG_DATE},
        dag=dag,
    ),
    ingest_cd2g = PythonOperator(
        task_id='ingest_call_drop_deuxg',
        provide_context=True,
        python_callable=extract_job,
        op_kwargs={'thetable': CONFIG["tables"][4]["name"],
                   'bucket': CONFIG["tables"][4]["bucket"],
                   'folder': CONFIG["tables"][4]["folder"],
                   'ingest_date': INGEST_PG_DATE},
        dag=dag,
    ),
    ingest_cd3g = PythonOperator(
        task_id='ingest_call_drop_troisg',
        provide_context=True,
        python_callable=extract_job,
        op_kwargs={'thetable': CONFIG["tables"][5]["name"],
                   'bucket': CONFIG["tables"][5]["bucket"],
                   'folder': CONFIG["tables"][5]["folder"],
                   'ingest_date': INGEST_PG_DATE},
        dag=dag,
    ),
    ingest_indis = PythonOperator(
        task_id='ingest_indisponibilite',
        provide_context=True,
        python_callable=extract_job,
        op_kwargs={'thetable': CONFIG["tables"][6]["name"],
                   'bucket': CONFIG["tables"][6]["bucket"],
                   'folder': CONFIG["tables"][6]["folder"],
                   'ingest_date': INGEST_PG_DATE},
        dag=dag,
    ),
    ingest_ftp = PythonOperator(
        task_id='ingest_ftp',
        provide_context=True,
        python_callable=extract_ftp_job,
        op_kwargs={'bucket': CONFIG["tables"][7]["bucket"],
                   'folder': CONFIG["tables"][7]["folder"],
                   'ingest_date': INGEST_FTP_DATE},
        dag=dag,
    )

    [ingest_hdrp, ingest_ts2g, ingest_ts3g, ingest_cd2g, ingest_cd3g, ingest_indis, ingest_ftp]

if __name__ == "__main__":
    from airflow.utils.state import State

    dag.clear()
    dag.run()