"""  EXTRACT DAG"""

from datetime import datetime, timedelta
from pathlib import Path
from minio import Minio
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor
from airflow.models import Variable
from airflow import DAG
from gps import CONFIG

from gps.common.extract import extract_pg, extract_ftp, list_ftp_file
from gps.common.rwminio import save_minio
from gps.common.alerting import send_email

#doc_md = Path(__file__).parents[1] / "docs/daily.md"

doc_md = """test doc airflow"""


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

SMTP_HOST = Variable.get('smtp_host')
SMTP_PORT = Variable.get('smtp_port')
SMTP_USER = Variable.get('smtp_user')


INGEST_PG_DATE = "{{ macros.ds_add(ds, -1) }}"
INGEST_FTP_DATE = "{{ macros.ds_add(ds, -6) }}"



CLIENT = Minio( MINIO_ENDPOINT,
        access_key= MINIO_ACCESS_KEY,
        secret_key= MINIO_SECRET_KEY,
        secure=False)


# def extract_job(**kwargs):
#     """
#         extract callable
#     """
#     ingest_date = datetime.strptime(kwargs["ingest_date"], CONFIG["date_format"])
#     thetable = kwargs["thetable"]
#     if ingest_date < datetime(2022,12,30) :
#         if thetable in ["faitalarme"]:
#             data = extract_pg(host = PG_HOST, database= PG_DB, user= PG_USER,
#                    password= PG_PASSWORD , table= thetable , date= kwargs["ingest_date"])
#             if not data.empty:
#                 save_minio(CLIENT, kwargs["bucket"],
#                         kwargs["folder"] , kwargs["ingest_date"], data)
#             else:
#                 raise RuntimeError(f"No data for {kwargs['ingest_date']}")
#     elif ( ingest_date > datetime(2022,12,29) ) and ( ingest_date <= datetime(2023,2,28) ):
#         if thetable in ["faitalarme", "hourly_datas_radio_prod"]:
#             if thetable == "hourly_datas_radio_prod":
#                 thetable = "hourly_datas_radio_prod_archive"
#             data = extract_pg(host = PG_HOST, database= PG_DB, user= PG_USER,
#                     password= PG_PASSWORD , table= thetable , date= kwargs["ingest_date"])

#             if not data.empty:
#                 save_minio(CLIENT, kwargs["bucket"], kwargs["folder"] , kwargs["ingest_date"], data)
#             else:
#                 raise RuntimeError(f"No data for {kwargs['ingest_date']}")
#     elif ( ingest_date >= datetime(2023,3,1) ) and ( ingest_date < datetime(2023,3,3) ):
#         if thetable in ["faitalarme", "hourly_datas_radio_prod"]:
#             data = extract_pg(host = PG_HOST, database= PG_DB, user= PG_USER,
#                     password= PG_PASSWORD , table= thetable , date= kwargs["ingest_date"])

#             if not data.empty:
#                 save_minio(CLIENT, kwargs["bucket"], kwargs["folder"] , kwargs["ingest_date"], data)
#             else:
#                 raise RuntimeError(f"No data for {kwargs['ingest_date']}")
#     else:
#         data = extract_pg(host = PG_HOST, database= PG_DB, user= PG_USER,
#             password= PG_PASSWORD , table= thetable , date= kwargs["ingest_date"])
#         if not data.empty:
#             save_minio(CLIENT, kwargs["bucket"], kwargs["folder"] , kwargs["ingest_date"], data)
#         else:
#             raise RuntimeError(f"No data for {kwargs['ingest_date']}")


def extract_job(**kwargs):
    data = extract_pg(host = PG_HOST, database= PG_DB, user= PG_USER,
            password= PG_PASSWORD , table= kwargs["thetable"] , date= kwargs["ingest_date"])
    if kwargs["thetable"] == "hourly_datas_radio_prod" and data.empty:
        data = extract_pg(host = PG_HOST, database= PG_DB, user= PG_USER,
            password= PG_PASSWORD , table = "hourly_datas_radio_prod_archive" , date= kwargs["ingest_date"])
    if  data.empty:
        raise RuntimeError(f"No data for {kwargs['ingest_date']}")
    
    save_minio(CLIENT, kwargs["bucket"], kwargs["folder"] , kwargs["ingest_date"], data)
            

def extract_ftp_job(**kwargs):
    """
    extract ftp files callable
 
    """
    data = extract_ftp(FTP_HOST, FTP_USER, FTP_PASSWORD, kwargs["ingest_date"])
    if  data.empty:
        raise RuntimeError(f"No data for {kwargs['ingest_date']}")
    save_minio(CLIENT, kwargs["bucket"], kwargs["folder"], kwargs["ingest_date"], data)
                

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
        'daily_extract',
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
        start_date=datetime(2023, 1, 1, 6, 30, 0),
        catchup=True
) as dag:
    check_file_sensor = PythonSensor(
        task_id= "sensor_ca",
        mode='poke',
        poke_interval= 24* 60 *60,
        timeout = 120 * 60 * 60,
        python_callable= check_file,
        doc_md = doc_md,
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
        doc_md = doc_md,
        trigger_rule='one_failed',  # Exécuter la tâche si le sensor échoue
        op_kwargs={
            'ingest_date': INGEST_FTP_DATE,
            'host': SMTP_HOST, 
            'port':SMTP_PORT,
            'users': SMTP_USER,
            'receivers': CONFIG["airflow_receivers"]
        }
    )

    tasks = []
    for table_config in CONFIG["tables"]:
        if table_config["name"] in ["faitalarme", "hourly_datas_radio_prod",  "Taux_succes_2g", "Taux_succes_3g"]:
            task_id = f'ingest_{table_config["name"]}'
            callable_fn = extract_job if table_config["name"] != "caparc" else extract_ftp_job
            INGEST_DATE = INGEST_PG_DATE if table_config["name"] != "caparc" else INGEST_FTP_DATE
            task = PythonOperator(
                task_id=task_id,
                provide_context=True,
                python_callable=callable_fn,
                doc_md = doc_md,
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
        if table_config["name"] == "caparc":
            task_id = f'ingest_{table_config["name"]}'
            callable_fn =  extract_ftp_job
            INGEST_DATE =  INGEST_FTP_DATE
            ingest_caparc = PythonOperator(
                task_id=task_id,
                provide_context=True,
                python_callable=callable_fn,
                doc_md = doc_md,
                op_kwargs={
                    'thetable': table_config["name"],
                    'bucket': table_config["bucket"],
                    'folder': table_config["folder"],
                    'table': table_config["table"],
                    'ingest_date': INGEST_DATE
                },
                dag=dag,
            )


    check_file_sensor >> send_email_task
    check_file_sensor >> ingest_caparc
    tasks