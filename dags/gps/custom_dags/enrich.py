# Monthly DAG
from datetime import datetime
import logging
from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.sensors.python import PythonSensor
from airflow.models import Variable
from minio import Minio
from gps import CONFIG
from gps.common.cleaning import (
    clean_base_sites,
    cleaning_esco,
    cleaning_ihs,
    cleaning_congestion,
    cleaning_ca_parc,
    cleaning_trafic_v2,
)
from gps.common.enrich import oneforall
from gps.common.alerting import alert_failure
from gps.common.rwminio import save_minio, get_latest_file
from gps.common.rwpg import write_pg
from gps.common.alerting import get_receivers, send_email
from gps.common.extract import extract_pg

PG_SAVE_HOST = Variable.get('pg_save_host')
PG_SAVE_DB = Variable.get('pg_save_db')
PG_SAVE_USER = Variable.get('pg_save_user')
PG_SAVE_PASSWORD = Variable.get('pg_save_password')

INGEST_DATE = "{{ macros.ds_add(ds, -1) }}"

MINIO_ENDPOINT =  Variable.get('minio_host')
MINIO_ACCESS_KEY = Variable.get('minio_access_key')
MINIO_SECRET_KEY = Variable.get('minio_secret_key')

SMTP_HOST = Variable.get('smtp_host')
SMTP_PORT = Variable.get('smtp_port')
SMTP_USER = Variable.get('smtp_user')


PG_HOST = Variable.get('pg_host')
PG_V2_DB = Variable.get('pg_v2_db')
PG_V2_USER = Variable.get('pg_v2_user')
PG_V2_PASSWORD = Variable.get('pg_v2_password')


DATE = "{{data_interval_start | ds}}"

CLIENT = Minio( MINIO_ENDPOINT,
        access_key= MINIO_ACCESS_KEY,
        secret_key= MINIO_SECRET_KEY,
        secure=False)   

def extract_trafic_v2(**kwargs):
    """
    """
    data = extract_pg(host = PG_HOST, database= PG_V2_DB, user= PG_V2_USER, 
            password= PG_V2_PASSWORD , table= kwargs["thetable"] , date= kwargs["date"])
    if  data.empty:
        raise RuntimeError(f"No data for {kwargs['date']}")
    save_minio(CLIENT, kwargs["bucket"] , kwargs["date"], data, kwargs["folder"])

def clean_trafic_v2(**kwargs):
    """
    """
    data = cleaning_trafic_v2(CLIENT, kwargs["date"])
    if  data.empty:
        raise RuntimeError(f"No data for {kwargs['date']}")
    save_minio(CLIENT, kwargs["bucket"], kwargs["date"], data, f'{kwargs["folder"]}-cleaned')


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
    if "cleaning_bdd" in params["task_id"]:
        params["type_fichier"] = "BASE_SITES"
    elif "cleaning_esco" in params["task_id"]:
        params["type_fichier"] = "OPEX_ESCO"
    elif "cleaning_ihs" in params["task_id"]:
        params["type_fichier"] = "OPEX_IHS"
    else:
        raise RuntimeError("Can't get file type")
    alert_failure(**params)

def check_file(**kwargs ):
    """
    check if file exists
    """
    if (kwargs['table_type'] == 'OPEX_IHS') and (kwargs["date"].split('-')[1] not in ["01", "04", "07", "10"]):
        return True
    
    table_obj = next((table for table in CONFIG["tables"] if table["name"] == kwargs['table_type'] ), None)
    date_parts = kwargs["date"].split("-")
    filename = get_latest_file(client=kwargs["client"], bucket=table_obj["bucket"], prefix=f"{table_obj['folder']}/{table_obj['folder']}_{date_parts[0]}{date_parts[1]}")
    if filename is None:
        return False
    return True
    
def gen_oneforall(**kwargs):
    data = oneforall(
        CLIENT,
        kwargs["endpoint"],
        kwargs["accesskey"],
        kwargs["secretkey"],
        kwargs["date"],
        kwargs["start_date"],
    )
    
    if not data.empty:
        logging.info("START TO SAVE INTO POSTGRES")
        write_pg(
            host=PG_SAVE_HOST,
            database=PG_SAVE_DB,
            user=PG_SAVE_USER,
            password=PG_SAVE_PASSWORD,
            data=data,
            table="motower_monthly")
        logging.info("START TO SAVE INTO MINIO")
        save_minio(client=CLIENT, bucket=CONFIG["final_bucket"], date=kwargs["date"], data=data, folder= CONFIG["final_folder"])
    else:
        raise RuntimeError(f"No data for {kwargs['date']}")

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


 # Set up DAG
with DAG(
    "enrich_prod",
    default_args={ 
        "depends_on_past": False,
        "email": CONFIG["airflow_receivers"],
        "email_on_failure": True,
        "email_on_retry": False,
        "max_active_tasks": 45,
        "retries": 0,
    },
    description="clean monthly data",
    schedule_interval="0 0 2 * *",
    start_date=datetime(2024, 7, 2, 0, 0, 0),
    catchup=True,
) as dag:
     # Task group for cleaning tasks
    with TaskGroup("cleaning", tooltip="Tasks for cleaning") as section_cleaning:
        table_config = next((table for table in CONFIG["tables"] if table["name"] == "ks_tdb_radio_drsi"), None)
        extract_trafic_deux = PythonOperator(
                task_id="extract_trafic_deux",
                provide_context=True,
                python_callable=extract_trafic_v2,
                op_kwargs={
                    'thetable': table_config["name"],
                    'bucket': table_config["bucket"],
                    'folder': table_config["folder"],
                    'table': table_config["table"],
                    'date': DATE
                },
                dag=dag,
            )
        clean_trafic_deux = PythonOperator(
            task_id="cleaning_trafic_deux",
            provide_context=True,
            python_callable=clean_trafic_v2,
            op_kwargs={
                'thetable': table_config["name"],
                'bucket': table_config["bucket"],
                'folder': table_config["folder"],
                "date": DATE,
            },
            on_failure_callback=on_failure,
            dag=dag,
        )
        check_bdd_sensor =  PythonSensor(
            task_id= "check_bdd_sensor",
            mode='poke',
            poke_interval= 24* 60 *60, # 1 jour
            timeout = 336 * 60 * 60,    # 6 jours
            python_callable= check_file,
            op_kwargs={
                  'client': CLIENT,
                  'table_type': 'BASE_SITES',
                  'date': DATE
            })
        
        send_email_bdd_task = PythonOperator(
        task_id='send_email_bdd',
        python_callable=send_email_onfailure,
        trigger_rule='one_failed',  # Exécuter la tâche si le sensor échoue
        op_kwargs={
            'date': DATE,
            'host': SMTP_HOST, 
            'port':SMTP_PORT,
            'users': SMTP_USER,
            'code': "BASE_SITES"
        }
        )
        
        clean_base_site = PythonOperator(
            task_id="cleaning_bdd",
            provide_context=True,
            python_callable=clean_base_sites,
            op_kwargs={
                "client": CLIENT,
                "endpoint": MINIO_ENDPOINT,
                "accesskey": MINIO_ACCESS_KEY,
                "secretkey": MINIO_SECRET_KEY,
                "date": DATE,
            },
            on_failure_callback=on_failure,
            dag=dag,
        )
        check_esco_sensor =  PythonSensor(
            task_id= "check_esco_sensor",
            mode='poke',
            poke_interval= 24* 60 *60,
            timeout = 336 * 60 * 60,
            python_callable= check_file,
            op_kwargs={
                  'client': CLIENT,
                  'table_type': 'OPEX_ESCO',
                  'date': DATE,
       
            })
        
        check_esco_annexe_sensor =  PythonSensor(
            task_id= "check_esco_annexe_sensor",
            mode='poke',
            poke_interval= 24* 60 *60,
            soft_fail = True,
            timeout = 336 * 60 * 60,
            python_callable= check_file,
            op_kwargs={
                  'client': CLIENT,
                  'table_type': 'ANNEXE_OPEX_ESCO',
                  'date': DATE,
            
            })
        
        send_email_esco_task = PythonOperator(
        task_id='send_email_esco',
        python_callable=send_email_onfailure,
        trigger_rule='one_failed',  # Exécuter la tâche si le sensor échoue
        op_kwargs={
            'date': DATE,
            'host': SMTP_HOST, 
            'port':SMTP_PORT,
            'users': SMTP_USER,
            'code': "OPEX_ESCO"
        }
        )
        
        clean_opex_esco = PythonOperator(
            task_id="cleaning_esco",
            provide_context=True,
            trigger_rule = 'all_done',
            python_callable=cleaning_esco,
            op_kwargs={
                "client": CLIENT,
                "endpoint": MINIO_ENDPOINT,
                "accesskey": MINIO_ACCESS_KEY,
                "secretkey": MINIO_SECRET_KEY,
                "date": DATE,
            },
            on_failure_callback=on_failure,
            dag=dag,
        )
        
        check_ihs_sensor =  PythonSensor(
            task_id= "check_ihs_sensor",
            mode='poke',
            poke_interval= 24* 60 *60,
            timeout = 336 * 60 * 60,
            python_callable= check_file,
            op_kwargs={
                  'client': CLIENT,
                  'table_type': 'OPEX_IHS',
                  'date': DATE,
          
            })
        send_email_ihs_task = PythonOperator(
        task_id='send_email_ihs',
        python_callable=send_email_onfailure,
        trigger_rule='one_failed',  # Exécuter la tâche si le sensor échoue
        op_kwargs={
            'date': DATE,
            'host': SMTP_HOST, 
            'port':SMTP_PORT,
            'users': SMTP_USER,
            'code': "OPEX_IHS"
        }
        )
        clean_opex_ihs = PythonOperator(
            task_id="cleaning_ihs",
            provide_context=True,
            python_callable=cleaning_ihs,
            op_kwargs={
                "client": CLIENT,
                "endpoint": MINIO_ENDPOINT,
                "accesskey": MINIO_ACCESS_KEY,
                "secretkey": MINIO_SECRET_KEY,
                "date": DATE,
            },
            on_failure_callback=on_failure,
            dag=dag,
        )
      
        clean_caparc = PythonOperator(
            task_id="cleaning_caparc",
            provide_context=True,
            python_callable=cleaning_ca_parc,
            op_kwargs={
                "client": CLIENT,
                "endpoint": MINIO_ENDPOINT,
                "accesskey": MINIO_ACCESS_KEY,
                "secretkey": MINIO_SECRET_KEY,
                'smtp_host': SMTP_HOST,
                'smtp_port': SMTP_PORT, 
                'smtp_user': SMTP_USER,
                "date": DATE,
            },
            dag=dag,
        )
        
        extract_trafic_deux >> clean_trafic_deux 
        check_bdd_sensor >> send_email_bdd_task 
        check_bdd_sensor >> clean_base_site
        check_esco_sensor >> send_email_esco_task
        [check_esco_sensor, check_esco_annexe_sensor] >> clean_opex_esco
        check_ihs_sensor >> send_email_ihs_task 
        check_ihs_sensor>> clean_opex_ihs
        [clean_caparc]
    # Task group for oneforall tasks
    with TaskGroup("oneforall", tooltip="Tasks for generate oneforall") as section_oneforall:
        merge_data = PythonOperator(
            task_id="join_data",
            provide_context=True,
            python_callable=gen_oneforall,
            op_kwargs={
                "endpoint": MINIO_ENDPOINT,
                "accesskey": MINIO_ACCESS_KEY,
                "secretkey": MINIO_SECRET_KEY,
                "date": DATE,
                "start_date": "2023-01-06",
            },
            dag=dag,
        )
        
        merge_data 
    extract_trafic_deux >> clean_trafic_deux >> section_oneforall
    check_bdd_sensor >> clean_base_site >> section_oneforall
    [check_esco_sensor, check_esco_annexe_sensor] >> clean_opex_esco  >> section_oneforall
    check_ihs_sensor>> clean_opex_ihs >> section_oneforall
    [clean_caparc] >> section_oneforall

 
  
    
    
