import streamlit as st
import pandas as pd

import utils as ut


def aux_file_metadata():
    col1, col2, col3, col4, col5, col6 = st.columns(6)
    cosa_a_return = "column_sizes"
    # Create a button in each column
    if col1.button("Column Sizes"):
        cosa_a_return = "column_sizes"
    if col2.button("Value Counts"):
        cosa_a_return = "value_counts"
    if col3.button("Null Value Counts"):
        cosa_a_return = "null_value_counts"
    if col4.button("NaN Value Counts"):
        cosa_a_return = "nan_value_counts"
    if col5.button("Lower Bounds"):
        cosa_a_return = "lower_bounds"
    if col6.button("Upper Bounds"):
        cosa_a_return = "upper_bounds"
    return cosa_a_return


def display_manifest_file(manifest_path: str):
    manifest = ut.read_avro_to_dataframe(manifest_path)
    select_columns = [
        "status", "snapshot_id", "data_sequence_number", "file_sequence_number"
    ]
    st.table(manifest[select_columns])
    metadata_to_view = aux_file_metadata()
    st.table(pd.DataFrame(manifest["data_file"][0][metadata_to_view]))


def display_single_snapshot_(table, selected_snapshot):
    manifest_list_file = table.snapshot_by_id(
        selected_snapshot
        ).manifest_list.strip("file://")
    manifest_list = ut.read_avro_to_dataframe(manifest_list_file)
    st.table(manifest_list.transpose())
    return manifest_list


def display_table_metadata(table):
    st.header("Metadata")
    st.table(ut.get_table_metadata(table))

    st.header("Snapshots")
    st.table(ut.get_table_snapshots(table))

    st.subheader("Select a snapshot, view the manifest list")

    snapshot_id_list = ut.get_snapshot_id_list(table)
    selected_snapshot = st.selectbox("Snapshot ID:", snapshot_id_list)
    manifest_list = display_single_snapshot_(table, selected_snapshot)

    st.subheader("Manifest file:")
    manifest_files_list = list(map(
        lambda x: x.strip("file://"),
        list(manifest_list["manifest_path"])
    ))
    manifest_path = st.selectbox("Manifest file:", manifest_files_list)

    if manifest_path:
        display_manifest_file(manifest_path)


def main():
    st.title("Iceberg Metadata Explorer")
    ut.setup_example_table()

    table_path = st.text_input(
        "Enter the path to your Iceberg table:",
        "taxi_dataset"
    )
    if table_path:
        table = ut.load_iceberg_table(table_path)
        display_table_metadata(table)


if __name__ == "__main__":
    main()
