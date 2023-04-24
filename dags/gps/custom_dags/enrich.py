from datetime import datetime
from airflow import DAG
from gps.common.enrich import cleaning_base_site, cleaning_esco, cleaning_ihs, cleaning_ca_parc, cleaning_alarm, cleaning_trafic, cleaning_call_drop
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from gps import CONFIG
from gps.common.alerting import alert_failure



MINIO_ENDPOINT = Variable.get('minio_host')
MINIO_ACCESS_KEY = Variable.get('minio_access_key')
MINIO_SECRET_KEY = Variable.get('minio_secret_key')

SMTP_HOST = Variable.get('smtp_host')
SMTP_PORT = Variable.get('smtp_port')
SMTP_USER = Variable.get('smtp_user')


DATE = "{{data_interval_start.strftime('%Y-%m-%d')}}"

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
    'enrich',
    default_args={
        'depends_on_past': False,
        'email': CONFIG["airflow_receivers"],
        'email_on_failure': True,
        'email_on_retry': False,
        'max_active_run': 1,
        'retries': 0
    },
    description='clean and enrich monthly data',
    schedule_interval= "0 0 6 * *",
    start_date=datetime(2022, 7, 6, 0, 0, 0),
    catchup=True
) as dag:

    clean_base_site = PythonOperator(
        task_id='cleaning_bdd',
        provide_context=True,
        python_callable=cleaning_base_site,
        op_kwargs={'endpoint': MINIO_ENDPOINT,
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
        op_kwargs={'endpoint': MINIO_ENDPOINT,
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
        op_kwargs={'endpoint': MINIO_ENDPOINT,
                   'accesskey': MINIO_ACCESS_KEY,
                   'secretkey': MINIO_SECRET_KEY,
                   'date': DATE},
        on_failure_callback = on_failure,
        dag=dag
    )
    clean_ca_parc = PythonOperator(
        task_id='cleaning_ca_parc',
        provide_context=True,
        python_callable=cleaning_ca_parc,
        op_kwargs={'endpoint': MINIO_ENDPOINT,
                   'accesskey': MINIO_ACCESS_KEY,
                   'secretkey': MINIO_SECRET_KEY,
                   'date': DATE},
        dag=dag
    )
    clean_alarm = PythonOperator(
        task_id='cleaning_alarm',
        provide_context=True,
        python_callable=cleaning_alarm,
        op_kwargs={'endpoint': MINIO_ENDPOINT,
                   'accesskey': MINIO_ACCESS_KEY,
                   'secretkey': MINIO_SECRET_KEY,
                   'date': DATE},
        dag=dag
    )
    clean_trafic = PythonOperator(
        task_id='cleaning_trafic',
        provide_context=True,
        python_callable=cleaning_trafic,
        op_kwargs={'endpoint': MINIO_ENDPOINT,
                   'accesskey': MINIO_ACCESS_KEY,
                   'secretkey': MINIO_SECRET_KEY,
                   'date': DATE},
        dag=dag
    )
    clean_call_drop = PythonOperator(
        task_id='cleaning_call_drop',
        provide_context=True,
        python_callable=cleaning_call_drop,
        op_kwargs={'endpoint': MINIO_ENDPOINT,
                   'accesskey': MINIO_ACCESS_KEY,
                   'secretkey': MINIO_SECRET_KEY,
                   'date': DATE},
        dag=dag
    )
    
    [clean_base_site, clean_opex_esco,clean_opex_ihs, clean_ca_parc, clean_alarm, clean_trafic, clean_call_drop]
