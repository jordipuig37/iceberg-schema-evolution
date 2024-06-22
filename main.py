import streamlit as st
from metadata_explorer import main as page_metadata_explorer


def main():
    # side bar for navigation. pages:
    #   * metadata explorer -> inprogress
    #   * schema evolution explorer
    #   * schema evolution management
    #   * upload data
    st.sidebar.title("Navigate")
    page = st.sidebar.radio(
        "Go to:", [
            "Metadata Explorer",
            "Schema Evolution Explorer",
            "Schema Evolution Manager",
            "Upload Data"
        ]
    )

    if page == "Metadata Explorer":
        page_metadata_explorer()
    else:
        pass


if __name__ == "__main__":
    main()