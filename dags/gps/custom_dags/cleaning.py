from datetime import datetime
from airflow import DAG
from airflow.models.baseoperator import chain

from gps.common.cleaning import clean_base_sites, cleaning_esco, cleaning_ihs, cleaning_alarm, cleaning_trafic, cleaning_cssr, cleaning_congestion
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from gps import CONFIG
from gps.common.alerting import alert_failure
from gps.common.rwminio import save_minio
from gps.common.rwpg import write_pg
from minio import Minio


MINIO_ENDPOINT = Variable.get('minio_host')
MINIO_ACCESS_KEY = Variable.get('minio_access_key')
MINIO_SECRET_KEY = Variable.get('minio_secret_key')

SMTP_HOST = Variable.get('smtp_host')
SMTP_PORT = Variable.get('smtp_port')
SMTP_USER = Variable.get('smtp_user')

PG_SAVE_HOST = Variable.get('pg_save_host')
PG_SAVE_DB = Variable.get('pg_save_db')
PG_SAVE_USER = Variable.get('pg_save_user')
PG_SAVE_PASSWORD = Variable.get('pg_save_password')

DATE = "{{data_interval_start.strftime('%Y-%m-%d')}}"
CLIENT = Minio( MINIO_ENDPOINT,
        access_key= MINIO_ACCESS_KEY,
        secret_key= MINIO_SECRET_KEY,
        secure=False)


def on_failure(context):
    """
        on failure function
    """
    params = {
        "host" : SMTP_HOST,
        "port" : SMTP_PORT,
        "user": SMTP_USER,
        "task_id" : context['task'].task_id,
        "dag_id" : context['task'].dag_id,
        "exec_date" : context.get('ts') ,
        "exception" : context.get('exception'),

    }
    if "cleaning_bdd" in params['task_id'] :
        params['type_fichier'] = "BASE_SITES"
    elif "cleaning_esco" in  params['task_id']:
        params['type_fichier'] = "OPEX_ESCO"
    elif "cleaning_ihs" in params['task_id']:
        params['type_fichier'] = "OPEX_IHS"
        
    else:
        raise RuntimeError("Can't get file type")
    
    alert_failure(**params)

with DAG(
    'clean',
    default_args={
        'depends_on_past': False,
        'email': CONFIG["airflow_receivers"],
        'email_on_failure': True,
        'email_on_retry': False,
        'max_active_run': 1,
        'retries': 0
    },
    description='clean monthly data',
    schedule_interval= "0 0 6 * *",
    start_date=datetime(2022, 7, 6, 0, 0, 0),
    catchup=True
) as dag:

    clean_base_site = PythonOperator(
        task_id='cleaning_bdd',
        provide_context=True,
        python_callable=clean_base_sites,
        op_kwargs={'client': CLIENT,
                   'endpoint': MINIO_ENDPOINT,
                    'accesskey': MINIO_ACCESS_KEY, 
                    'secretkey': MINIO_SECRET_KEY,
                   'date': DATE},
        on_failure_callback = on_failure,
        dag=dag
    ),
    clean_opex_esco = PythonOperator(
        task_id='cleaning_esco',
        provide_context=True,
        python_callable=cleaning_esco,
        op_kwargs={'client': CLIENT,
                   'endpoint': MINIO_ENDPOINT,
                    'accesskey': MINIO_ACCESS_KEY, 
                    'secretkey': MINIO_SECRET_KEY,
                   'date': DATE},
        on_failure_callback = on_failure,
        dag=dag
    ),
    clean_opex_ihs = PythonOperator(
        task_id='cleaning_ihs',
        provide_context=True,
        python_callable=cleaning_ihs,
        op_kwargs={'client': CLIENT,
                   'endpoint': MINIO_ENDPOINT,
                    'accesskey': MINIO_ACCESS_KEY, 
                    'secretkey': MINIO_SECRET_KEY,
                   'date': DATE},
        on_failure_callback = on_failure,
        dag=dag
    ),
    # clean_ca_parc = PythonOperator(
    #     task_id='cleaning_ca_parc',
    #     provide_context=True,
    #     python_callable=cleaning_ca_parc,
    #     op_kwargs={'client': CLIENT,
    #                'date': DATE},
    #     dag=dag
    # )
    clean_alarm = PythonOperator(
        task_id='cleaning_alarm',
        provide_context=True,
        python_callable=cleaning_alarm,
        op_kwargs={'client': CLIENT,
                   'endpoint': MINIO_ENDPOINT,
                    'accesskey': MINIO_ACCESS_KEY, 
                    'secretkey': MINIO_SECRET_KEY,
                   'date': DATE},
        dag=dag
    ),
    clean_trafic = PythonOperator(
        task_id='cleaning_trafic',
        provide_context=True,
        python_callable=cleaning_trafic,
        op_kwargs={'client': CLIENT,
                   'endpoint': MINIO_ENDPOINT,
                    'accesskey': MINIO_ACCESS_KEY, 
                    'secretkey': MINIO_SECRET_KEY,
                   'date': DATE},
        dag=dag
    ),
    clean_cssr = PythonOperator(
        task_id='cleaning_cssr',
        provide_context=True,
        python_callable=cleaning_cssr,
        op_kwargs={'client': CLIENT,
                   'endpoint': MINIO_ENDPOINT,
                    'accesskey': MINIO_ACCESS_KEY, 
                    'secretkey': MINIO_SECRET_KEY,
                   'date': DATE},
        dag=dag
    ),
    clean_congestion = PythonOperator(
        task_id='cleaning_congestion',
        provide_context=True,
        python_callable=cleaning_congestion,
        op_kwargs={'client': CLIENT,
                   'endpoint': MINIO_ENDPOINT,
                    'accesskey': MINIO_ACCESS_KEY, 
                    'secretkey': MINIO_SECRET_KEY,
                   'date': DATE},
        dag=dag
    )
    
    # clean_call_drop = PythonOperator(
    #     task_id='cleaning_call_drop',
    #     provide_context=True,
    #     python_callable=cleaning_call_drop,
    #     op_kwargs={'endpoint': MINIO_ENDPOINT,
    #                'accesskey': MINIO_ACCESS_KEY,
    #                'secretkey': MINIO_SECRET_KEY,
    #                'date': DATE},
    #     dag=dag
    # )
    
    #chain([clean_base_site, clean_opex_esco,clean_opex_ihs, clean_ca_parc, clean_alarm, clean_trafic], merge_data)
    [clean_base_site, clean_opex_esco, clean_opex_ihs, clean_alarm, clean_trafic, clean_cssr, clean_congestion]  #, ,clean_opex_ihs, clean_ca_parc, clean_alarm, clean_trafic, clean_cssr]