import streamlit as st
import pandas as pd
from pyiceberg.table import Table
from st_aggrid import AgGrid, GridOptionsBuilder

import utils as ut


def display_metadata(table: Table):
    """This function displays the basic metadata of a table. The data shown is
    the same data that can be found in the metadata .json
    """
    metadata = ut.get_table_metadata(table)
    metadata_df = pd.DataFrame(filter(
            lambda t: type(t[1]) is str or type(t[1]) is int,
            metadata.items()
    ))

    metadata_df = metadata_df.set_index(0).drop(
        index=["last_sequence_number", "format_version",
               "default_sort_order_id", "last_partition_id",
               "default_spec_id", "last_column_id"]
    )

    metadata_df.loc["last_updated_ms"] = pd.to_datetime(
            metadata_df.loc["last_updated_ms"], unit="ms"
        ).dt.strftime("%Y-%m-%d %H:%M:%S")

    schema_df = ut.get_table_last_schema(table)

    n_schemas = len(metadata["schemas"])
    n_snapshots = len(metadata["snapshots"])

    col1, col2 = st.columns(2)
    col1.subheader("Metadata")
    col1.table(metadata_df)
    col1.text(f"Number of schemas: {n_schemas}")
    col1.text(f"Number of snapshots: {n_snapshots}")

    col2.subheader("Schema")
    schema_df = schema_df.set_index("id")
    col2.table(schema_df)


def display_snapshots(table: Table):
    """Display the history of snapshots."""
    snapshot_data = ut.get_table_snapshots(table)
    snapshot_data = snapshot_data.drop(columns="summary")
    snapshot_data["parent-snapshot-id"] = \
        snapshot_data["parent-snapshot-id"].fillna(0).astype('int64')
    snapshot_data["manifest-list"] = snapshot_data["manifest-list"].apply(
        lambda x: x.split("metadata/")[1])
    snapshot_data["timestamp-ms"] = pd.to_datetime(
            snapshot_data["timestamp-ms"], unit="ms"
        ).dt.strftime("%Y-%m-%d %H:%M:%S")

    st.table(snapshot_data.head())


def display_single_snapshot(table: Table, snapshot_id: int):
    """Displays some details of a given snapshot."""
    summary = table.snapshot_by_id(snapshot_id).model_dump()["summary"]
    summary_df = ut.process_summary(summary)
    # made this to fix having a 2 column table when using wide mode
    col1, _ = st.columns(2)

    col1.subheader("Summary")
    col1.table(summary_df.transpose())

    st.subheader("Manifest List")
    manifest_list_file = table.snapshot_by_id(
        snapshot_id
        ).manifest_list.strip("file://")
    mfst_list = ut.read_avro_to_dataframe(manifest_list_file)
    mfst_list["manifest_path"] = mfst_list["manifest_path"].apply(
        lambda x: x.split("metadata/")[1]
    )

    mfst_list["manifest_length"] = mfst_list["manifest_length"].apply(
        ut.format_number_with_spaces)
    mfst_list["added_rows_count"] = mfst_list["added_rows_count"].apply(
        ut.format_number_with_spaces)
    mfst_list["deleted_rows_count"] = mfst_list["deleted_rows_count"].apply(
        ut.format_number_with_spaces)
    mfst_list["existing_rows_count"] = mfst_list["existing_rows_count"].apply(
        ut.format_number_with_spaces)

    mfst_list = mfst_list.drop(
        columns=[  # [ ] consider if one of this is actually relevant
            "partition_spec_id", "content", "min_sequence_number",
            "partitions", "key_metadata"
        ]
    )
    mfst_list = mfst_list.reset_index()
    gb = GridOptionsBuilder.from_dataframe(mfst_list)
    gb.configure_column("index", pinned="left")
    grid_options = gb.build()

    # Display the table
    AgGrid(mfst_list, gridOptions=grid_options)


def aux_manifest_file_metadata(total_files):
    """Creates a 6 column button to select which column from manifest data_file
    is shown in the next table.
    """
    col0, col1, col2, col3, col4, col5, col6 = st.columns(7)

    file_num = col0.selectbox("Data file index", range(total_files))
    data_file_col = "column_sizes"
    # Create a button in each column
    if col1.button("Column Sizes"):
        data_file_col = "column_sizes"
    if col2.button("Value Counts"):
        data_file_col = "value_counts"
    if col3.button("Null Value Counts"):
        data_file_col = "null_value_counts"
    # TODO: process the missing data_file columns
    # if col4.button("NaN Value Counts"):
    #     data_file_col = "nan_value_counts"
    # if col5.button("Lower Bounds"):
    #     data_file_col = "lower_bounds"
    # if col6.button("Upper Bounds"):
    #     data_file_col = "upper_bounds"
    return data_file_col, file_num


def aux_get_manifest_files_list(table, selected_snapshot) -> pd.DataFrame:
    manifest_list_file = table.snapshot_by_id(
        selected_snapshot
        ).manifest_list.strip("file://")
    manifest_list = ut.read_avro_to_dataframe(manifest_list_file)
    manifest_files_list = list(map(
        lambda x: x.split("metadata/")[1],
        list(manifest_list["manifest_path"])
    ))
    assert len(list(manifest_files_list)) > 0
    base_path = ("/".join(list(manifest_list["manifest_path"])[0]
                          .split("/")[:-1])).strip("file://") + "/"

    return base_path, manifest_files_list


def aux_data_file_to_display(
        data_file_to_display: pd.DataFrame,
        metadata_to_view: str
        ) -> pd.DataFrame:
    # data_file_to_display = data_file.copy()
    if metadata_to_view.endswith("counts"):
        data_file_to_display["value"] = \
            data_file_to_display["value"].apply(ut.format_number_with_spaces)
    elif metadata_to_view.endswith("bounds"):
        pass  # TODO: finish this
    elif metadata_to_view.endswith("sizes"):
        data_file_to_display["value"] = \
            data_file_to_display["value"].apply(ut.convert_bytes)

    return data_file_to_display


def display_manifest_file(table: Table, manifest_path: str):
    manifest = ut.read_avro_to_dataframe(manifest_path)
    manifest["file_path"] = manifest["data_file"].apply(
        lambda x: x["file_path"].split("data/")[2]
    )
    manifest["record_count"] = manifest["data_file"].apply(
        lambda x: ut.format_number_with_spaces(x["record_count"])
    )
    manifest["file_size_in_bytes"] = manifest["data_file"].apply(
        lambda x: ut.convert_bytes(x["file_size_in_bytes"])
    )
    select_columns = [
        "status", "snapshot_id", "file_path", "record_count",
        "file_size_in_bytes"
    ]
    st.table(manifest[select_columns])

    data_file_column, file_num = aux_manifest_file_metadata(len(manifest))
    data_file = pd.DataFrame(manifest["data_file"][file_num][data_file_column])
    rename_key_dict = ut.get_key_to_column_name_mapping(table)
    data_file["key"] = data_file["key"].map(rename_key_dict)

    display_data_file = aux_data_file_to_display(data_file, data_file_column)
    st.table(display_data_file.set_index("key"))


def explore_table_metadata(table: Table):
    """Main function that given a table displays the interface to explore
    the main components of the iceberg table metadata.
    """
    st.header("Overview")
    display_metadata(table)

    st.header("Snapshots")
    display_snapshots(table)

    st.subheader("Select a snapshot, view the manifest list")
    st.text("This is the summary of the last operation, that generated a new snapshot.")  # noqa: E501
    snapshot_id_list = ut.get_snapshot_id_list(table)
    selected_snapshot: str = st.selectbox("Snapshot ID:", snapshot_id_list)
    display_single_snapshot(table, selected_snapshot)

    base_metadata_path, manifest_files_list = \
        aux_get_manifest_files_list(table, selected_snapshot)

    st.subheader("Manifest file:")
    manifest_path = st.selectbox("Manifest file:", manifest_files_list)

    if manifest_path:
        display_manifest_file(table, base_metadata_path + manifest_path)


def main():
    st.title("Iceberg Metadata Explorer")

    table_path = st.text_input(
        "Enter the path to your Iceberg table:",
        "taxi_dataset"
    )
    if table_path:
        table = ut.load_iceberg_table(table_path)
        explore_table_metadata(table)


if __name__ == "__main__":
    main()
