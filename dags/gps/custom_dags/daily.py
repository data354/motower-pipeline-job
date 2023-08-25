from datetime import datetime, timedelta
from pathlib import Path
from minio import Minio
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor
from airflow.models import Variable
from airflow import DAG
from gps import CONFIG
from gps.common.extract import extract_ftp, list_ftp_file
from gps.common.rwminio import save_minio
from gps.common.alerting import send_email


FTP_HOST = Variable.get('ftp_host')
FTP_USER = Variable.get('ftp_user')
FTP_PASSWORD = Variable.get('ftp_password')

INGEST_FTP_DATE = "{{ macros.ds_add(ds, -2) }}"

MINIO_ENDPOINT = Variable.get('minio_host')
MINIO_ACCESS_KEY = Variable.get('minio_access_key')
MINIO_SECRET_KEY = Variable.get('minio_secret_key')

SMTP_HOST = Variable.get('smtp_host')
SMTP_PORT = Variable.get('smtp_port')
SMTP_USER = Variable.get('smtp_user')

CLIENT = Minio( MINIO_ENDPOINT,
        access_key= MINIO_ACCESS_KEY,
        secret_key= MINIO_SECRET_KEY,
        secure=False)


def extract_ftp_job(**kwargs):
    """
    extract ftp files callable
 
    """
    data = extract_ftp(FTP_HOST, FTP_USER, FTP_PASSWORD, kwargs["ingest_date"])
    if  data.empty:
        raise RuntimeError(f"No data for {kwargs['ingest_date']}")
    save_minio(CLIENT, kwargs["bucket"], kwargs["ingest_date"], data, kwargs["folder"])

def check_file(**kwargs):
    """
        check if file exists
    """
    filename = f"extract_vbm_{kwargs['ingest_date'].replace('-', '')}.csv"
    liste = list_ftp_file(FTP_HOST, FTP_USER, FTP_PASSWORD)
    if filename in liste:
        return True
    return False  


def send_email_onfailure(**kwargs):
    """
    send email if sensor failed
    """
    filename = f"extract_vbm_{kwargs['ingest_date'].replace('-', '')}.csv"
    subject = f"  Missing file {filename}"
    content = f"Missing file {filename}. please provide file asap"
    send_email(kwargs["host"], kwargs["port"], kwargs["users"], kwargs["receivers"], subject, content)
with DAG(
        'daily',
        default_args={
            'depends_on_past': False,
            'wait_for_downstream': False,
            'email': CONFIG["airflow_receivers"],
            'email_on_failure': True,
            'email_on_retry': False,
            'max_active_run': 1,
            'retries': 0
        },
        description='daily job',
        schedule_interval="0 20 * * *",
        start_date=datetime(2023, 7, 9, 6, 30, 0),
        catchup=True
) as dag:
    check_file_sensor = PythonSensor(
        task_id= "sensor_ca",
        mode=None,
        python_callable= check_file,
        op_kwargs={
        #     'hostname': FTP_HOST,
        #     'user': FTP_USER,
        #     'password': FTP_PASSWORD,
              'ingest_date': INGEST_FTP_DATE,
        #     'smtp_host': SMTP_HOST,
        #     'smtp_user': SMTP_USER,
        #     'smtp_port': SMTP_PORT,
        #     'receivers': CONFIG["airflow_receivers"]
        },

    )
    send_email_task = PythonOperator(
        task_id='send_email',
        python_callable=send_email_onfailure,
        trigger_rule='one_failed',  # Exécuter la tâche si le sensor échoue
        op_kwargs={
            'ingest_date': INGEST_FTP_DATE,
            'host': SMTP_HOST, 
            'port':SMTP_PORT,
            'users': SMTP_USER,
            'receivers': CONFIG["airflow_receivers"]
        }
    )
    table_config = next((table for table in CONFIG["tables"] if table["name"] == "caparc"), None)
    get_caparc = PythonOperator(
                task_id= "get_caparc",
                provide_context=True,
                python_callable=extract_ftp_job,
                op_kwargs={
                    'thetable': table_config["name"],
                    'bucket': table_config["bucket"],
                    'folder': table_config["folder"],
                    'table': table_config["table"],
                    'ingest_date': INGEST_FTP_DATE
                },
                dag=dag,
            )
    check_file_sensor >> send_email_task 
    check_file_sensor >> get_caparc