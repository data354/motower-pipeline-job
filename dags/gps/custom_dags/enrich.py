# from datetime import datetime
# from airflow import DAG
# from airflow.models.baseoperator import chain

# from gps.common.enrich import oneforall, get_last_ofa
# from airflow.operators.python import PythonOperator
# from airflow.models import Variable
# from minio import Minio


# from gps import CONFIG
# from gps.common.alerting import alert_failure
# from gps.common.rwminio import save_minio
# from gps.common.rwpg import write_pg


# MINIO_ENDPOINT = Variable.get('minio_host')
# MINIO_ACCESS_KEY = Variable.get('minio_access_key')
# MINIO_SECRET_KEY = Variable.get('minio_secret_key')

# SMTP_HOST = Variable.get('smtp_host')
# SMTP_PORT = Variable.get('smtp_port')
# SMTP_USER = Variable.get('smtp_user')

# PG_SAVE_HOST = Variable.get('pg_save_host')
# PG_SAVE_DB = Variable.get('pg_save_db')
# PG_SAVE_USER = Variable.get('pg_save_user')
# PG_SAVE_PASSWORD = Variable.get('pg_save_password')

# DATE = "{{data_interval_start.strftime('%Y-%m-%d')}}"



# CLIENT = Minio( MINIO_ENDPOINT,
#         access_key= MINIO_ACCESS_KEY,
#         secret_key= MINIO_SECRET_KEY,
#         secure=False)


# with DAG(
#     'enrich',
#     default_args={
#         'depends_on_past': False,
#         'email': CONFIG["airflow_receivers"],
#         'email_on_failure': True,
#         'email_on_retry': False,
#         'max_active_run': 1,
#         'retries': 0
#     },
#     description=' enrich monthly data',
#     schedule_interval= "0 0 6 * *",
#     start_date=datetime(2023, 1, 6, 0, 0, 0),
#     catchup=True
# ) as dag:
#     merge_data   = PythonOperator(
#         task_id='join_data',
#         provide_context=True,
#         python_callable=gen_oneforall,
#         # op_kwargs={'endpoint': MINIO_ENDPOINT,
#                    'accesskey': MINIO_ACCESS_KEY,
#                    'secretkey': MINIO_SECRET_KEY,
#                    'date': DATE,
#                    'start_date' : "2023-01-06"},
#         dag=dag
#     ),
#     save_pg = PythonOperator(
#         task_id='save_pg',
#         provide_context=True,
#         python_callable=save_in_pg,
#         op_kwargs={'endpoint': MINIO_ENDPOINT,
#                    'accesskey': MINIO_ACCESS_KEY,
#                    'secretkey': MINIO_SECRET_KEY,
#                    'date': DATE
#                    },
#         dag=dag
#     ),


#     merge_data >> save_pg