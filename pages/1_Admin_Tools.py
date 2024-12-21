import streamlit as st
import json
import pandas as pd
from typing import Dict
from databricks_utils import make_api_request, load_workspace_config, setup_workspace_selector

st.set_page_config(layout="wide")

def get_metastore_details(workspace_info: Dict) -> Dict:
    """
    Get metastore details for a specific workspace
    """
    return make_api_request(workspace_info, "/api/2.1/unity-catalog/metastores")

def get_metastore_id(metastore_details):
    return metastore_details['metastores'][0]['metastore_id']

def get_system_table_status(workspace_info: Dict) -> pd.DataFrame:
    """
    Get system table status for a specific metastore
    Returns a DataFrame containing the system schemas
    """
    metastore_details = get_metastore_details(workspace_info)
    metastore_id = get_metastore_id(metastore_details)
    response_data = make_api_request(workspace_info, f"/api/2.1/unity-catalog/metastores/{metastore_id}/systemschemas")
    system_table_df = pd.DataFrame(response_data.get('schemas', []))
    return system_table_df

def enable_system_schema(workspace_info: Dict, metastore_id: str, schema_name: str) -> Dict:
    """
    Enable a system schema for a specific metastore
    """
    endpoint = f"/api/2.1/unity-catalog/metastores/{metastore_id}/systemschemas/{schema_name}"
    response_data = make_api_request(workspace_info, endpoint, method="PUT")
    if response_data:
        st.success(f"System schema {schema_name} enabled successfully")
    return response_data

st.title("Workspace Admin Tools")

uploaded_file = st.file_uploader("Choose JSON file with Workspace details", type=["json"])

if uploaded_file is not None:
    try:
        data = json.load(uploaded_file)
        
        if not isinstance(data, list):
            st.error("Invalid JSON format. Expected a list of workspace configurations.")
        else:
            # Create a list of URLs from the workspace configurations
            urls = [workspace['url'] for workspace in data]
            selected_url = st.selectbox("Select Workspace URL", urls)
            
            # Find the selected workspace configuration
            selected_workspace = next((workspace for workspace in data if workspace['url'] == selected_url), None)

            if st.button("List Metastores"):
                if selected_workspace:
                    st.info(f"Fetching metastore details for {selected_url}")
                    metastore_details = get_metastore_details(selected_workspace)
                    if metastore_details:
                        metastore_details_df = pd.DataFrame(metastore_details.get('metastores', []))
                        st.dataframe(metastore_details_df, hide_index=True)
                    else:
                        st.warning("No metastore details found")
            
            # Get metastore ID from the first metastore in the list
            if st.button("Get System Schema Status"):
                system_table_df = get_system_table_status(selected_workspace)
                if not system_table_df.empty:
                    st.dataframe(system_table_df, hide_index=True,use_container_width=True)
                else:
                    st.warning("No system schema status data found")

            # st.write("Select System Table to Enable")
            show_schema = st.checkbox("Show available schemas to Enable")
            if show_schema:
                system_table_df = get_system_table_status(selected_workspace)
                filtered_df = system_table_df[system_table_df['state'] == "AVAILABLE"]
                available_tables = filtered_df["schema"].tolist()
            # st.write(available_tables)
            
                if 'table_select' not in st.session_state:
                        st.session_state.table_select = None
                st.session_state.table_select = st.selectbox("Select System Schema to Enable", available_tables)
                if st.button("Enable System Schema"):
                    if st.session_state.table_select:  # Check if a table was selected
                        st.info(f"Enabling system schema: {st.session_state.table_select}")
                        metastore_details = get_metastore_details(selected_workspace)
                        metastore_id = get_metastore_id(metastore_details)
                        try:
                            response = enable_system_schema(selected_workspace,metastore_id,st.session_state.table_select)
                        except Exception as e:
                            st.error(f"Error enabling system table kkk: {str(e)}")

    except json.JSONDecodeError:
        st.error("Invalid JSON file")
    except Exception as e:
        st.error(f"An error occurred: {str(e)}")
