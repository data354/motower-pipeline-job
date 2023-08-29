from datetime import datetime
from minio import Minio
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor
from airflow.models import Variable
from airflow import DAG
from gps import CONFIG
from gps.common.extract import extract_ftp, list_ftp_file
from gps.common.rwminio import save_minio
from gps.common.alerting import send_email, alert_failure, get_receivers
from gps.common.daily import motower_daily, cleaning_daily_trafic
from gps.common.rwpg import write_pg
from gps.custom_dags.weekly import extract_v2

# get variables

FTP_HOST = Variable.get('ftp_host')
FTP_USER = Variable.get('ftp_user')
FTP_PASSWORD = Variable.get('ftp_password')

PG_SAVE_HOST = Variable.get('pg_save_host')
PG_SAVE_DB = Variable.get('pg_save_db')
PG_SAVE_USER = Variable.get('pg_save_user')
PG_SAVE_PASSWORD = Variable.get('pg_save_password')

INGEST_DATE = "{{ macros.ds_add(ds, -1) }}"

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
    date_parts = kwargs["date"].split("-")
    filename = f"{kwargs['code']}_{date_parts[0]}{date_parts[1]}.xlsx"
    subject = f"  Missing file {filename}"
    content = f"Missing file {filename}. please provide file asap"
    receivers = get_receivers(code=kwargs["code"])
     
    send_email(kwargs["host"], kwargs["port"], kwargs["users"], receivers, subject, content)

def gen_motower_daily(**kwargs):
    data = motower_daily(
        CLIENT,
        kwargs["endpoint"],
        kwargs["accesskey"],
        kwargs["secretkey"],
        kwargs["date"]    )
    if not data.empty:
        write_pg(PG_SAVE_HOST, PG_SAVE_DB, PG_SAVE_USER, PG_SAVE_PASSWORD, data, "motower_daily")
    else:
        raise RuntimeError(f"No data for {kwargs['date']}")

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
        start_date=datetime(2023, 7, 2, 6, 30, 0),
        catchup=True
) as dag:
    check_file_sensor = PythonSensor(
        task_id= "sensor_ca",
        mode="reschedule",
        retries=0,
        python_callable= check_file,
        op_kwargs={
      
              'ingest_date': INGEST_DATE,
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
            'ingest_date': INGEST_DATE,
            'host': SMTP_HOST, 
            'port':SMTP_PORT,
            'users': SMTP_USER,
            'code': ' '  # renvoie le mail de JEAN LOUIS
        }
    )
    table_config = next((table for table in CONFIG["tables"] if table["name"] == "caparc"), None)
    get_caparc = PythonOperator(
                task_id= "get_caparc",
                provide_context=True,
                python_callable=extract_ftp_job,
                on_failure_callback=on_failure,
                op_kwargs={
                    'thetable': table_config["name"],
                    'bucket': table_config["bucket"],
                    'folder': table_config["folder"],
                    'table': table_config["table"],
                    'ingest_date': INGEST_DATE
                },
                dag=dag,
            )
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
                    'ingest_date': INGEST_DATE
                },
                dag=dag,
            )
    clean_trafic_task = PythonOperator(
            task_id="clean_trafic_task",
            provide_context=True,
            python_callable=cleaning_daily_trafic,
            on_failure_callback=on_failure,
            op_kwargs={
                "client": CLIENT,
                "endpoint": MINIO_ENDPOINT,
                "accesskey": MINIO_ACCESS_KEY,
                "secretkey": MINIO_SECRET_KEY,
                "date": INGEST_DATE,
            },
            # on_failure_callback=on_failure,
            dag=dag,
        )
    motower_task = PythonOperator(
            task_id="motower_task",
            provide_context=True,
            python_callable=gen_motower_daily,
            on_failure_callback=on_failure,
            op_kwargs={
                "endpoint": MINIO_ENDPOINT,
                "accesskey": MINIO_ACCESS_KEY,
                "secretkey": MINIO_SECRET_KEY,
                "date": INGEST_DATE,
            },
            dag=dag,
        )
    [check_file_sensor >> send_email_task , extract_trafic >> clean_trafic_task]
    [check_file_sensor >> get_caparc , extract_trafic>> clean_trafic_task] >> motower_task