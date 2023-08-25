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


doc_md = """

## DAILY DAG (EXTRACT)

### PURPOSE
Ce workflow continent 6 tâches et est chargé en général d’extraire des daily data et les stocker dans des fichiers CSV dans Minio. 
La path de stockage dans MINIO est: bucket_name/folder/année/mois/jour.csv

Elle interagit donc avec les daily sources (postgreSQl et FTP) et MINIO. Elle s’execute quotidiennement à 5h30. Les configurations concernant les sources sont contenues dans des variables airflow.

### TASKS
- ingest_Taux_succes_2g: Cette tâche est chargée de recupérer les données du success rate pour la technologie 2G. La source de données est postgreSQL (Table: Taux_succes_2g). Le bucket Minio est success-rate et le folder 2g;

- ingest_Taux_succes_3g: Cette tâche est chargée de recupérer les données du success rate pour la technologie 3G. La source de données est postgreSQL (Table: Taux_succes_3g). Le bucket Minio est success-rate et le folder 3g;
- Ingest_faitalarme: Cette tâche quant à elle récupère les données liées à l’indisponibilité. La source de données est postgreSQL (Table: faitalarme); Le bucket Minio est indisponibilite et le folder indisponibilite;
- Ingest_hourly_datas_radio_prod: recupère les données concernant le trafic. La source de données est postgreSQL (Table: hourly_datas_radio_prod). Le bucket Minio est trafic et le folder trafic
- Ingest_caparc: Cette tâche recupère les données concernant le CA et le PARC via FTP. La particularité de cette tâche est qu’elle recupère les données de d-7. Le declenchement de cette tâche depend du sensor_ca. Sensor_ca est une tâche spéciale qui verifie la présence du fichier à recupérer quotidiennement. Dès que le sensor dectecte la présence dufichier, il passe à succès et la tâche est exécutée. Au bout de 5 jours, si le fichier n’est toujours pas fourni dans la source,le sensor echoue et la tâche send_email est exécutée. Le bucket Minio est caparc et le folder caparc;
- Send_email: envoie un mail au responsable du fichier CA et PARC afin qu’il fournissent le fichier manquant


"""


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





def extract_job(**kwargs):
    data = extract_pg(host = PG_HOST, database= PG_DB, user= PG_USER,
            password= PG_PASSWORD , table= kwargs["thetable"] , date= kwargs["ingest_date"])
    if kwargs["thetable"] == "hourly_datas_radio_prod" and data.empty:
        data = extract_pg(host = PG_HOST, database= PG_DB, user= PG_USER,
            password= PG_PASSWORD , table = "hourly_datas_radio_prod_archive" , date= kwargs["ingest_date"])
    if  data.empty:
        raise RuntimeError(f"No data for {kwargs['ingest_date']}")
    
    save_minio(CLIENT, kwargs["bucket"] , kwargs["ingest_date"], data, kwargs["folder"])
            

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