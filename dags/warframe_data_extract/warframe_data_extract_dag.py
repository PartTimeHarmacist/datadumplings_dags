import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator

DAG_ID = "warframe_data_extract"

with DAG(
    dag_id=DAG_ID,
    schedule_interval="0 * * * *",
    start_date=datetime.datetime(2024, 12, 17),
    catchup=False
) as dag:

    t0a = EmptyOperator(task_id="start")

    t1 = EmptyOperator(task_id="load_data")

    t2 = EmptyOperator(task_id="transform_data")

    t3 = EmptyOperator(task_id="upload_data")

    t0b = EmptyOperator(task_id="end")

    t0a >> t1 >> t2 >> t3 >> t0b
