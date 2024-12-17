import streamlit as st
import json
import requests
from typing import Dict
import pandas as pd

st.set_page_config(layout="wide")

def get_metastore_details(workspace_info: Dict) -> Dict:
    """
    Get metastore details for a specific workspace
    """
    url = f"{workspace_info['url']}/api/2.1/unity-catalog/metastores"
    headers = {
        "Authorization": f"Bearer {workspace_info['token']}",
        "Content-Type": "application/json"
    }
    
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        st.error(f"Error querying metastore for {workspace_info['url']}: {str(e)}")
        return {}

def get_metastore_id(metastore_details):
    return metastore_details['metastores'][0]['metastore_id']

def get_system_table_status(workspace_info: Dict) -> pd.DataFrame:
    """
    Get system table status for a specific metastore
    Returns a DataFrame containing the system schemas
    """
    metastore_details = get_metastore_details(workspace_info)
    metastore_id = get_metastore_id(metastore_details)
    url = f"{workspace_info['url']}/api/2.1/unity-catalog/metastores/{metastore_id}/systemschemas"
    headers = {
        "Authorization": f"Bearer {workspace_info['token']}",
        "Content-Type": "application/json"
    }
    
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        system_table_status = response.json()
        system_table_df = pd.DataFrame(system_table_status.get('schemas', []))
        return system_table_df
    except Exception as e:
        st.error(f"Error getting system schema status: {str(e)}")
        return pd.DataFrame()

def enable_system_schema(workspace_info: Dict, metastore_id: str, schema_name: str) -> Dict:
    """
    Enable a system schema for a specific metastore
    """
    url = f"{workspace_info['url']}/api/2.1/unity-catalog/metastores/{metastore_id}/systemschemas/{schema_name}"
    headers = {
        "Authorization": f"Bearer {workspace_info['token']}",
        "Content-Type": "application/json"
    }
    
    try:
        response = requests.put(url, headers=headers)
        response.raise_for_status()
        st.success(f"System schema {schema_name} enabled successfully")
        return response.json()
    except Exception as e:
        st.error(f"Error enabling system schema: {str(e)}")
        return {}

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
                    st.dataframe(system_table_df, hide_index=True)
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
