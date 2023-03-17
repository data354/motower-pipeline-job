from pathlib import Path
from datetime import datetime
import yaml
from airflow import DAG
from gps.common.extract import extract, save_minio
from airflow.operators.python import PythonOperator


config_file = Path(__file__).parents[3] / "config/configs.yaml"
if config_file.exists():
    with config_file.open("r",) as f:
        config = yaml.safe_load(f)
else:
    raise RuntimeError("configs file don't exists")


def extract_job(table: str):
    """
        extract
    """
    logical_date = "{{ds}}"

    data = extract(table, logical_date)

    if data.shape[0] == 0:
        save_minio(table, logical_date, data)
    else:
        raise RuntimeError(f"No data for {logical_date}")


with DAG(
    'etl_dag_test_local',
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
    start_date=datetime(2023, 3, 13, 5, 30, 0),
    catchup=False
) as dag:

    ingest_hdrp = PythonOperator(
        task_id='ingest hourly_datas_radio_prod',
        provide_context=True,
        python_callable=extract_job,
        op_args={'table': config["tables"][0]},
        dag=dag,
    ),

    ingest_ts2g = PythonOperator(
        task_id='ingest Taux_succes_2g',
        provide_context=True,
        python_callable=extract_job,
        op_args={'table': config["tables"][1]},
        dag=dag,
    ),

    ingest_ts3g = PythonOperator(
        task_id='ingest Taux_succes_3g',
        provide_context=True,
        python_callable=extract_job,
        op_args={'table': config["tables"][2]},
        dag=dag,
    ),
    ingest_ts3g = PythonOperator(
        task_id='ingest Taux_succes_3g',
        provide_context=True,
        python_callable=extract_job,
        op_args={'table': config["tables"][2]},
        dag=dag,
    ),

    ingest_hdrp

if __name__ == "__main__":
    from airflow.utils.state import State

    dag.clear()
    dag.run()
