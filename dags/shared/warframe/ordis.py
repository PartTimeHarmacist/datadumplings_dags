from dataclasses import dataclass, field
import requests
from bs4 import Tag, PageElement, BeautifulSoup
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
        self.drop_table_records = []


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
            drop_tables[header_text] = []

            # Break after hitting the sorties header, as the ItemsBySource is not yet implemented
            if "sorties" in header_text.strip().lower():
                break

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
