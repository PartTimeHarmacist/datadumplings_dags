import datetime
import logging

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.models.taskinstance import TaskInstance
from airflow.models.variable import Variable

DAG_ID = "warframe_data_extract"

from dataclasses import dataclass
from bs4 import BeautifulSoup
import requests
import re


@dataclass
class Chance:
    description: str
    percentage: float

@dataclass
class Drop:
    name: str
    chance: Chance

@dataclass
class Stage:
    primary: int
    group: int = None
    # Stage 1, Stage 2, Stage 3 of 4, Stage 3 of 5, etc

    def __str__(self):
        rtn = f"Stage {self.primary}"
        if self.group:
            rtn += f" of {self.group}"
        return rtn

@dataclass
class Selector:
    name: str
    rotation: str
    stage: Stage = None

@dataclass
class DropTableRecord:
    drop_table_type: str
    selector: Selector
    drop: Drop

@dataclass
class ItemBySource:
    selector: str
    resource_drop_chance: float
    drop: Drop


class DropTableProcessor:
    pct_re = re.compile(r"\((?P<pct>.*)%\)$")

    def __init__(
            self,
            url: str
    ):
        self.url = url
        self.drop_table_records = {}


    @staticmethod
    def _split_on_blank_rows(rows: list) -> list[list]:
        current_group = []
        for row in rows:
            if 'blank-row' in row.get("class", []):
                if current_group:
                    yield current_group
                    current_group = []
            else:
                current_group.append(row)


    def load_data(self):
        resp = requests.get(self.url)
        soup = BeautifulSoup(resp.text, "html.parser")
        drop_tables = {}

        for table in soup.select("table"):
            header = table.find_previous_sibling("h3")
            header_text = header.text.rstrip(":")

            # Break after hitting the sorties header, as the ItemsBySource is not yet implemented
            if "sorties" in header_text.strip().lower():
                break

            drop_tables[header_text] = []


            # Iterate over the types
            for drop_table in self._split_on_blank_rows(table.select("tr")):
                # Each drop table is a list of rows
                selector = None
                drop_table_records = []
                for row in drop_table:
                    if th := row.select("th"):
                        th = th[0]
                        if th.attrs.get("colspan"):
                            if "rotation" in th.text.strip().lower():
                                rotation = th.text[-1]
                                selector.rotation = rotation
                            else:
                                selector_text = th.text
                                selector = Selector(
                                    selector_text,
                                    rotation=""
                                )
                    else:
                        row_data = row.select("td")
                        chance_text = row_data[-1].text
                        drop_text = row_data[-2].text.strip()

                        chance_desc, chance_amt = chance_text.rsplit("(", 1)
                        chance_desc = chance_desc.strip()
                        chance_amt = chance_amt[:-2]
                        chance_amt = float(chance_amt)

                        drop = Drop(
                            drop_text,
                            Chance(
                                chance_desc,
                                chance_amt
                            )
                        )

                        drop_table_records.append(
                            DropTableRecord(
                                drop_table_type = header_text,
                                selector = selector,
                                drop = drop
                            )
                        )
                drop_tables[header_text] = drop_tables[header_text] + drop_table_records


        self.drop_table_records = drop_tables



def load_data(ti: TaskInstance, **kwargs):
    import requests
    from bs4 import BeautifulSoup, Tag, PageElement


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

        try:
            drop_table_type = DropTableType.from_html_table(table, header_text)
        except ValueError as e:
            logging.warning(f"Header {header_text} failed to import!", exc_info=e)
            continue

        logging.info(f"Header: {header_text}")
        if num_drop_tables := len(drop_table_type.drop_tables) > 0:
            logging.info(f"Number of drop tables: {len(drop_table_type.drop_tables)}")

            for tbl in drop_table_type.drop_tables:
                logging.debug(tbl.name)
        else:
            logging.warning("No drop tables for header!")





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
        retries=2,
        retry_delay=datetime.timedelta(seconds=15),
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
