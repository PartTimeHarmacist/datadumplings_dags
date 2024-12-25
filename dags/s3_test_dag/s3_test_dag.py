from airflow import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.models.taskinstance import TaskInstance

import datetime
import time

DAG_ID = "s3_test_dag"

def upload_to_s3(ti: TaskInstance):
    time.sleep(5)
    s3 = S3Hook(aws_conn_id="s3_datadumplings")
    s3.load_string(
        f"Test Content - Loaded at {datetime.datetime.now(datetime.UTC)}",
        key="test-file.log",
        bucket_name="test",
        replace=True
    )
    time.sleep(10)

with DAG(
    dag_id=DAG_ID,
    schedule_interval="0 * * * *",
    start_date=datetime.datetime(2024, 12, 24),
    catchup=False
) as dag:

    dag.doc_md = """
    # S3 Test DAG
    Just a simple DAG to test the connection and setup of the local S3 deployment, by overwriting a singular log file
    with the current time in UTC.
    """

    t0a = EmptyOperator(task_id="start")

    t1 = PythonOperator(
        task_id="upload_test",
        python_callable=upload_to_s3,
        op_kwargs={},
        op_args=[]
    )

    t0b = EmptyOperator(task_id="end")

t0a >> t1 >> t0b
