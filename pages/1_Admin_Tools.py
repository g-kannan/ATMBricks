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
                        st.dataframe(metastore_details_df,hide_index=True)
                    else:
                        st.warning("No metastore details found")
    
    except json.JSONDecodeError:
        st.error("Invalid JSON file")
    except Exception as e:
        st.error(f"An error occurred: {str(e)}")
