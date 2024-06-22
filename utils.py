import pandas as pd
import os
from fastavro import reader
from pyiceberg.table import Table
from pyiceberg.catalog import load_catalog
from pyiceberg.catalog.sql import SqlCatalog
import pyarrow.parquet as pq
import pyarrow.compute as pc
import sqlite3
import json
from typing import List


WAREHOUSE_PATH = "./data/warehouse"


def create_or_replace_catalog(setup_func):
    def wrapper():
        try:  # try to connect to the catalog
            catalog = load_catalog(
                "default",
                **{
                    "uri": f"sqlite:///{WAREHOUSE_PATH}/pyiceberg_catalog.db",
                    "warehouse": f"file://{WAREHOUSE_PATH}",
                },
            )
            assert catalog
        except:
            os.mkdir(WAREHOUSE_PATH)
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


def get_table_metadata(ib_table: Table) -> pd.DataFrame:
    mtdt = json.loads(ib_table.metadata.json())
    metadata_df = pd.DataFrame(filter(
            lambda t: type(t[1]) is str or type(t[1]) is int,
            mtdt.items()
    ))

    return metadata_df


def get_table_snapshots(ib_table: Table):
    snapshot_history = ib_table.history()
    res = list()
    for snapshot in snapshot_history:
        res.append(json.loads(
            ib_table.snapshot_by_id(snapshot.snapshot_id).model_dump_json()
        ))

    return pd.DataFrame(res).transpose()


def get_snapshot_id_list(table: Table) -> List[str]:
    snapshot_history = table.history()
    return list(map(lambda snp: snp.snapshot_id, snapshot_history))


def get_table_manifest_list(ib_table, snapshot_id):
    pass
