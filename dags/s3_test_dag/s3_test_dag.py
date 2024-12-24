from airflow import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.models.taskinstance import TaskInstance

import datetime

DAG_ID = "s3_test_dag"

def upload_to_s3(ti: TaskInstance):
    s3 = S3Hook(aws_conn_id="s3_datadumplings")
    s3.load_string("Test Content", key="test-file.log", bucket_name="test")

with DAG(
    dag_id=DAG_ID,
    schedule_interval="0 * * * *",
    start_date=datetime.datetime(2024, 12, 24),
    catchup=False
) as dag:

    t0a = EmptyOperator(task_id="start")

    t1 = PythonOperator(
        task_id="upload_test",
        python_callable=upload_to_s3,
        op_kwargs={},
        op_args=[]
    )

    t0b = EmptyOperator(task_id="end")

t0a >> t1 >> t0b
