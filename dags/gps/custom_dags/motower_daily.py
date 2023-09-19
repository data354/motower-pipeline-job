from datetime import datetime
from functools import partial
from minio import Minio
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor
from airflow.models import Variable
from airflow import DAG
from gps import CONFIG
from gps.common.extract import extract_ftp, list_ftp_file, extract_pg
from gps.common.rwminio import save_minio
from gps.common.alerting import send_email, alert_failure, get_receivers
from gps.common.motower_daily import generate_daily_caparc, cleaning_daily_trafic, cleaning_congestion
from gps.common.rwpg import write_pg

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

PG_HOST = Variable.get('pg_host')
PG_DB = Variable.get('pg_db')
PG_USER = Variable.get('pg_user')
PG_PASSWORD = Variable.get('pg_password')

PG_HOST = Variable.get('pg_host')
PG_V2_DB = Variable.get('pg_v2_db')
PG_V2_USER = Variable.get('pg_v2_user')
PG_V2_PASSWORD = Variable.get('pg_v2_password')


CLIENT = Minio( MINIO_ENDPOINT,
        access_key= MINIO_ACCESS_KEY,
        secret_key= MINIO_SECRET_KEY,
        secure=False)



################################### FUNCTIONS

def check_data_in_table(**kwargs):

    data = extract_pg(host = PG_HOST, database= PG_V2_DB, user= PG_V2_USER, 
            password= PG_V2_PASSWORD , table= kwargs["thetable"] , date= kwargs['ingest_date'])
    if data.empty:
        return False
    return True


def extract_pg_job(**kwargs):
    """
    """
    data = extract_pg(host = PG_HOST, database= PG_V2_DB, user= PG_V2_USER, 
            password= PG_V2_PASSWORD , table= kwargs["thetable"] , date= kwargs['ingest_date'])
    if  data.empty:
        raise RuntimeError(f"No data for {kwargs['ingest_date']}")
    
    save_minio(CLIENT, kwargs["bucket"] , kwargs['ingest_date'], data, kwargs["folder"])

def clean_trafic(**kwargs):
    """
    """
    data = cleaning_daily_trafic(CLIENT, MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, kwargs['ingest_date'])
    if  data.empty:
        raise RuntimeError(f"No data for {kwargs['ingest_date']}")
    write_pg(PG_SAVE_HOST, PG_SAVE_DB, PG_SAVE_USER, PG_SAVE_PASSWORD, data, "motower_daily_trafic")

def clean_congestion(**kwargs):
    """
    """
    data = cleaning_congestion(CLIENT, MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, kwargs['ingest_date'])
    if  data.empty:
        raise RuntimeError(f"No data for {kwargs['ingest_date']}")
    write_pg(PG_SAVE_HOST, PG_SAVE_DB, PG_SAVE_USER, PG_SAVE_PASSWORD, data, "motower_daily_congestion")
    
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
    date_parts = kwargs["ingest_date"].split("-")
    filename = f"{kwargs['code']}_{date_parts[0]}{date_parts[1]}.xlsx"
    subject = f"  Missing file {filename}"
    content = f"Missing file {filename}. please provide file asap"
    receivers = get_receivers(code=kwargs["code"])
    send_email(kwargs["host"], kwargs["port"], kwargs["users"], receivers, subject, content)

def gen_motower_daily(**kwargs):
    data = generate_daily_caparc(
        CLIENT,
        MINIO_ENDPOINT,
        MINIO_ACCESS_KEY,
        MINIO_SECRET_KEY,
        kwargs["ingest_date"],
        PG_SAVE_HOST, 
        PG_SAVE_USER, 
        PG_SAVE_PASSWORD, PG_SAVE_DB)
    if not data.empty:
        write_pg(PG_SAVE_HOST, PG_SAVE_DB, PG_SAVE_USER, PG_SAVE_PASSWORD, data, "motower_daily_caparc")
    else:
        raise RuntimeError(f"No data for {kwargs['ingest_date']}")

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
    alert_failure(**params)



######################################### DAG DEFINITIONS
with DAG(
        'motower_daily',
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
        schedule_interval="0 6 * * *",
        start_date=datetime(2023, 7, 2, 6, 0, 0),
        catchup=True
) as dag:
    check_file_sensor = PythonSensor(
        task_id= "sensor_ca",
        mode="poke",
        retries=0,
        timeout=10,
        python_callable= check_file,
        on_failure_callback = on_failure,
        op_kwargs={
      
              'ingest_date': INGEST_DATE,
        },

    )
    send_email_task = PythonOperator(
        task_id='send_email',
        python_callable=send_email_onfailure,
        trigger_rule='one_failed',  # Exécuter la tâche si le sensor échoue
        on_failure_callback=on_failure,
        op_kwargs={
            'ingest_date': INGEST_DATE,
            'host': SMTP_HOST, 
            'port':SMTP_PORT,
            'users': SMTP_USER,
            'code': 'CA_SITES'  
        }
    )
    table_config = next((table for table in CONFIG["tables"] if table["name"] == "caparc"), None)
    ingest_caparc = PythonOperator(
                task_id='ingest_caparc',
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

    generate_motower_dcaparc = PythonOperator(
            task_id="motower_dcaparc",
            provide_context=True,
            python_callable=gen_motower_daily,
            on_failure_callback=on_failure,
            depends_on_past= True,
            op_kwargs={
                "ingest_date": INGEST_DATE
            },
            dag=dag,
    )

    table_config = next((table for table in CONFIG["tables"] if table["name"] == "ks_daily_tdb_radio_drsi"), None)

    check_trafic_sensor = PythonSensor(
        task_id= "sensor_trafic",
        mode="poke",
        poke_interval=24* 60 *60, # 1 jour
        timeout=168* 60 *60, #7 jours
        python_callable= check_data_in_table,
        on_failure_callback = on_failure,
        op_kwargs={
             'thetable': table_config["name"],
              'ingest_date': INGEST_DATE,
        },
    )
    send_email_trafic_task = PythonOperator(
        task_id='send_email_trafic',
        python_callable=send_email_onfailure,
        trigger_rule='one_failed',  # Exécuter la tâche si le sensor échoue
        on_failure_callback=on_failure,
        op_kwargs={
            'ingest_date': INGEST_DATE,
            'host': SMTP_HOST, 
            'port':SMTP_PORT,
            'users': SMTP_USER,
            'code': 'trafic'  
        }
    )
    extract_trafic = PythonOperator(
                task_id="extract_trafic",
                provide_context=True,
                python_callable=extract_pg_job,
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
            python_callable=clean_trafic,
            on_failure_callback=on_failure,
            op_kwargs={
                "client": CLIENT,
                'bucket': table_config["bucket"],
                'folder': table_config["folder"],
                'table': table_config["table"],
                "ingest_date": INGEST_DATE,
            },
            dag=dag,
        )
    table_config = next((table for table in CONFIG["tables"] if table["name"] == "ks_hebdo_tdb_radio_drsi"), None)
    check_congestion_sensor = PythonSensor(
        task_id= "sensor_congestion",
        mode="poke",
        poke_interval=24* 60 *60, # 1 jour
        timeout=168* 60 *60, #7 jours
        python_callable= check_data_in_table,
        on_failure_callback = on_failure,
        op_kwargs={
             'thetable': table_config["name"],
              'ingest_date': INGEST_DATE,
        },
    )
    send_email_congestion_task = PythonOperator(
        task_id='send_email_congestion',
        python_callable=send_email_onfailure,
        trigger_rule='one_failed',  # Exécuter la tâche si le sensor échoue
        on_failure_callback=on_failure,
        op_kwargs={
            'ingest_date': INGEST_DATE,
            'host': SMTP_HOST, 
            'port':SMTP_PORT,
            'users': SMTP_USER,
            'code': 'congestion'  
        }
    )
    extract_congestion_task = PythonOperator(
                task_id="extract_congestion",
                provide_context=True,
                python_callable=extract_pg_job,
                op_kwargs={
                    'thetable': table_config["name"],
                    'bucket': table_config["bucket"],
                    'folder': table_config["folder"],
                    'table': table_config["table"],
                    'ingest_date': INGEST_DATE
                },
                dag=dag,
    )
    clean_congestion_task = PythonOperator(
            task_id="gen_congestion_task",
            provide_context=True,
            python_callable=clean_congestion,
            op_kwargs={
                'thetable': table_config["name"],
                'bucket': table_config["bucket"],
                'folder': table_config["folder"],
                'table': table_config["table"],
                'ingest_date': INGEST_DATE
            },
            #on_failure_callback=on_failure,
            dag=dag,
        )
    check_file_sensor >> send_email_task  
    check_file_sensor >> ingest_caparc >> generate_motower_dcaparc
    check_trafic_sensor >> send_email_trafic_task
    check_trafic_sensor >> extract_trafic >> clean_trafic_task
    check_congestion_sensor >> send_email_congestion_task
    check_congestion_sensor>> extract_congestion_task >> clean_congestion_task
    


    
    
    
    








    