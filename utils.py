import pandas as pd
import os
from fastavro import reader
from pyiceberg.table import Table
from pyiceberg.catalog import load_catalog
from pyiceberg.catalog.sql import SqlCatalog
import pyarrow.parquet as pq
import pyarrow.compute as pc
import json
from typing import List


WAREHOUSE_PATH = "./data/warehouse"


def create_or_replace_catalog(setup_func):
    def wrapper():
        if not os.path.exists(WAREHOUSE_PATH):
            os.mkdir(WAREHOUSE_PATH)
        try:  # try to connect to the catalog and the table
            catalog = load_catalog(
                "default",
                **{
                    "uri": f"sqlite:///{WAREHOUSE_PATH}/pyiceberg_catalog.db",
                    "warehouse": f"file://{WAREHOUSE_PATH}",
                },
            )
            assert catalog

            tbl = load_iceberg_table("taxi_dataset")
            assert tbl
        except:  # noqa: E722
            setup_func()
    return wrapper


@create_or_replace_catalog
def setup_example_table():
    # Create a catalog
    catalog = SqlCatalog(
        "default",
        **{
            "uri": f"sqlite:///{WAREHOUSE_PATH}/pyiceberg_catalog.db",
            "warehouse": f"file://{WAREHOUSE_PATH}",
        },
    )
    catalog.create_namespace("default")

    # read parquet data
    df = pq.read_table("./data/yellow_tripdata_2023-01.parquet")

    # create table
    table = catalog.create_table(
        "default.taxi_dataset",
        schema=df.schema,
    )
    # put data from dataframe to iceberg table
    table.append(df)

    # add a new column to the dataframe
    df = df.append_column(
        "tip_per_mile",
        pc.divide(df["tip_amount"], df["trip_distance"])
    )
    # make an update to the schema
    with table.update_schema() as update_schema:
        update_schema.union_by_name(df.schema)
    # put that new data into the table
    table.overwrite(df)
    df = table.scan(row_filter="tip_per_mile > 0").to_arrow()


def read_avro_to_dataframe(file_path: str) -> pd.DataFrame:
    """Reads an avro file and returns it as a pandas DataFrame.
    """
    with open(file_path, 'rb') as f:
        return pd.DataFrame(
            [record for record in reader(f)]
        )


def load_iceberg_table(table_name: str) -> Table:
    catalog = load_catalog(
        "default",
        **{
            "uri": f"sqlite:///{WAREHOUSE_PATH}/pyiceberg_catalog.db",
            "warehouse": f"file://{WAREHOUSE_PATH}",
        },
    )
    return catalog.load_table(f"default.{table_name}")


def get_table_metadata(table: Table) -> dict:
    """Returns the metadata of the table in a dictionary."""
    mtdt = json.loads(table.metadata.json())
    return mtdt


def get_table_snapshots(table: Table) -> pd.DataFrame:
    """Get """
    snapshot_history = table.history()
    res = list()
    for snapshot in snapshot_history:
        res.append(json.loads(
            table.snapshot_by_id(snapshot.snapshot_id).model_dump_json()
        ))

    return pd.DataFrame(res)


def get_snapshot_id_list(table: Table) -> List[str]:
    snapshot_history = table.history()
    return list(map(lambda snp: snp.snapshot_id, snapshot_history))


def get_table_manifest_list(ib_table, snapshot_id):
    pass


def get_table_last_schema(table: Table) -> pd.DataFrame:
    return pd.DataFrame(table.schema().model_dump()["fields"])


def get_key_to_column_name_mapping(table: Table):
    return dict(map(
        lambda fld: (fld.field_id, fld.name),
        table.schema().fields
    ))


def get_snapshot_summary(table: Table, snapshot_id: int) -> dict:
    return table.snapshot_by_id(snapshot_id).model_dump()["summary"]


def convert_bytes(size_in_bytes: int) -> str:
    """Convert size from bytes to MB or GB."""
    if size_in_bytes >= 1024**3:  # 1 GB in bytes
        size_in_gb = size_in_bytes / 1024**3
        return f"{size_in_gb:.2f} GB"
    else:
        size_in_mb = size_in_bytes / 1024**2  # 1 MB in bytes
        return f"{size_in_mb:.2f} MB"


def format_number_with_spaces(number: int):
    """Format a number string by separating thousands with a space."""
    return f"{number:,}".replace(",", " ")


def process_summary(summary: dict[str, str]) -> pd.DataFrame:
    """Applies logic"""
    new_summer = dict()
    for k, v in summary.items():
        if k.endswith("-size"):
            new_summer[k] = convert_bytes(int(v))
        elif k.endswith("-records"):
            new_summer[k] = format_number_with_spaces(int(v))
        else:
            new_summer[k] = v

    summary_df = pd.DataFrame(new_summer, index=["Value"])
    return summary_df
