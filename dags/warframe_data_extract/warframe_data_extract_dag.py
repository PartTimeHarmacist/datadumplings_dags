import datetime
import logging

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.models.taskinstance import TaskInstance
from airflow.models.variable import Variable

DAG_ID = "warframe_data_extract"


def load_data(ti: TaskInstance, **kwargs):
    import requests
    from bs4 import BeautifulSoup, Tag, PageElement

    try:
        from shared.warframe.ordis import DropTableType
    except ImportError:
        # Dev environment
        from dags.shared.warframe.ordis import DropTableType


    drop_table_url = Variable.get(kwargs.get("drop_table_url_var", "WARFRAME_DROP_TABLE_URL"))

    resp = requests.get(drop_table_url)
    soup = BeautifulSoup(resp.text, "html.parser")

    # Data is split by h3 headers into categories
    # Reward Tables are each a table where the first header is formatted like so:
    # {planet}/{node} ({mission type})
    # Followed by headers for each rotation
    # Until a blank row is encountered (class="blank-row"), where the pattern repeats
    for table in soup.select("table"):
        header = table.find_previous_sibling("h3")
        header_text = header.text.rstrip(":")

        drop_table_type = DropTableType.from_html_table(table, header_text)

        logging.info(f"Header: {header_text}")
        logging.info(f"Number of drop tables: {len(drop_table_type.drop_tables)}")

        for tbl in drop_table_type.drop_tables:
            logging.info(tbl.name)





def transform_data(ti: TaskInstance):
    pass

def upload_data(ti: TaskInstance):
    pass


with DAG(
    dag_id=DAG_ID,
    schedule_interval="0 * * * *",
    start_date=datetime.datetime(2024, 12, 17),
    catchup=False
) as dag:

    t0a = EmptyOperator(task_id="start")

    t1 = PythonOperator(
        task_id="load_data",
        python_callable=load_data,
        op_kwargs={
            "drop_table_url_var": "WARFRAME_DROP_TABLE_URL"
        }
    )

    t2 = PythonOperator(
        task_id="transform_data",
        python_callable=transform_data
    )

    t3 = PythonOperator(
        task_id="upload_data",
        python_callable=upload_data
    )

    t0b = EmptyOperator(task_id="end")

    t0a >> t1 >> t2 >> t3 >> t0b
