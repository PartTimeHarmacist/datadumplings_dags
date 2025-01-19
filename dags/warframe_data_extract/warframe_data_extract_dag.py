import datetime
import logging

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.models.taskinstance import TaskInstance
from airflow.models.variable import Variable

DAG_ID = "warframe_data_extract"

from dataclasses import dataclass, field
from bs4 import Tag, PageElement
import re


@dataclass
class Reward:
    """A reward on a drop table"""
    name: str
    rarity: str
    percentage: float

    @classmethod
    def from_html_table_row(cls, tr: Tag):
        properties_re = re.compile(r"(?P<rarity>.*)\s\((?P<percentage>\d{1,3}\.?\d{,2})%\)")
        name, properties = tr.select("td")
        name = name.text.strip()
        properties = properties_re.match(properties.text).groupdict()
        return cls(
            name=name,
            rarity=properties["rarity"].strip(),
            percentage=float(properties["percentage"])
        )

    def __str__(self):
        return f"{self.name} - {self.rarity} ({self.percentage:.2f}%)"

@dataclass
class Table:
    name: str
    rewards: list[Reward]

    @classmethod
    def from_list_of_tags(cls, tag_list: list[Tag]):
        pass


@dataclass
class Rotation(Table):
    @classmethod
    def from_list_of_tags(cls, tag_list: list[Tag]):
        header = tag_list[0]
        tag_list = tag_list[1:]
        return cls(
            name=header.text.strip(),
            rewards=[Reward.from_html_table_row(t) for t in tag_list]
        )


@dataclass
class DropTable(Table):
    """For tracking a drop table from the official Warframe Drop tables"""
    rotations: list[Rotation] = field(default_factory=list)
    rewards: list[Reward] = field(default_factory=list)

    @classmethod
    def from_list_of_tags(cls, tag_list: list[Tag]):
        rewards = []
        rotations = []
        # First tag is the header
        name = tag_list[0].text.strip()
        tag_list = tag_list[1:]
        if "rotation" in tag_list[0].text.strip().lower():
            # We have rotations
            rotation_list = [tag_list[0]]
            for tag in tag_list[1:]:
                if "rotation" in tag.text.strip().lower():
                    if rotation_list:
                        # We have a list of rotation items, thus we've hit a new rotation
                        rotations.append(Rotation.from_list_of_tags(rotation_list))
                        rotation_list = [tag]
                        continue
                rotation_list.append(tag)
        else:
            # We have a direct rewards list
            rewards = [Reward.from_html_table_row(t) for t in tag_list]

        return cls(
            name=name,
            rotations=rotations,
            rewards=rewards
        )


@dataclass
class DropTableType:
    """A collection of related drop tables (relics, missions, etc)"""
    name: str
    drop_tables: list[DropTable] = field(default_factory=list)

    @staticmethod
    def _split_on_blank_rows(rows: list[Tag]) -> list[list[Tag]]:
        current_group = []
        for row in rows:
            if 'blank-row' in row.get('class', []):
                if current_group:
                    yield current_group
                    current_group = []
            else:
                current_group.append(row)

    @classmethod
    def from_html_table(cls, t: Tag, header: str):
        return cls(
            name=header.rstrip(":"),
            drop_tables=[DropTable.from_list_of_tags(d) for d in cls._split_on_blank_rows(t.select("tr"))]
        )



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
