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

def get_system_table_status(workspace_info: Dict, metastore_id: str) -> Dict:
    """
    Get system table status for a specific metastore
    """
    url = f"{workspace_info['url']}/api/2.1/unity-catalog/metastores/{metastore_id}/systemschemas"
    headers = {
        "Authorization": f"Bearer {workspace_info['token']}",
        "Content-Type": "application/json"
    }
    
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        st.error(f"Error getting system table status: {str(e)}")
        return {}

st.title("Workspace Admin Tools")

uploaded_file = st.file_uploader("Choose a JSON file", type=["json"])

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
                    st.info(f"Fetching metastore details for {selected_url}...")
                    metastore_details = get_metastore_details(selected_workspace)
                    if metastore_details:
                        metastore_details_df = pd.DataFrame(metastore_details.get('metastores', []))
                        st.dataframe(metastore_details_df, hide_index=True)
                    else:
                        st.warning("No metastore details found")
            
            # Get metastore ID from the first metastore in the list
            if st.button("Get System Table Status"):
                metastore_details = get_metastore_details(selected_workspace)
                metastore_id = metastore_details['metastores'][0]['metastore_id']
                st.success(f"Metastore ID: {metastore_id}")
                st.info("Fetching system table status...")
                system_table_status = get_system_table_status(selected_workspace,metastore_id)
                if system_table_status:
                    system_table_df = pd.DataFrame(system_table_status.get('schemas', []))
                    st.dataframe(system_table_df, hide_index=True)


    except json.JSONDecodeError:
        st.error("Invalid JSON file")
    except Exception as e:
        st.error(f"An error occurred: {str(e)}")
