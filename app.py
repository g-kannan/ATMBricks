import streamlit as st
import json
import pandas as pd
import requests
from typing import List, Dict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import pytz
import duckdb

st.set_page_config(layout="wide")
def query_clusters(workspace_info: Dict) -> List[Dict]:
    """
    Query clusters for a specific workspace
    """
    url = f"{workspace_info['url']}/api/2.1/clusters/list"
    headers = {
        "Authorization": f"Bearer {workspace_info['token']}",
        "Content-Type": "application/json"
    }
    
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        clusters = response.json().get('clusters', [])
        
        # Add workspace information to each cluster
        for cluster in clusters:
            cluster['workspace_url'] = workspace_info['url']
            cluster['environment'] = workspace_info['environment']
            
        return clusters
    except Exception as e:
        st.error(f"Error querying {workspace_info['url']}: {str(e)}")
        return []

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

def process_workspaces(workspaces: List[Dict]) -> pd.DataFrame:
    """
    Process multiple workspaces in parallel and combine results
    """
    all_clusters = []
    
    with ThreadPoolExecutor(max_workers=10) as executor:
        future_to_workspace = {
            executor.submit(query_clusters, workspace): workspace 
            for workspace in workspaces
        }
        
        for future in as_completed(future_to_workspace):
            clusters = future.result()
            all_clusters.extend(clusters)
    
    if not all_clusters:
        return pd.DataFrame()
    
    return pd.DataFrame(all_clusters)

def select_and_convert_times(df: pd.DataFrame) -> pd.DataFrame:
    """
    Select only certain columns and convert starttime, terminated_time and last_restarted_time to UTC
    """
    columns = [
        "cluster_name",
        "cluster_id",
        "creator_user_name",
        "environment",
        "autotermination_minutes",
        "spark_version",
        "runtime_engine",
        "state",
        "start_time",
        "terminated_time",
        "last_restarted_time",
        "autoscale"
    ]
    
    conn = duckdb.connect()
    df = conn.execute("""
        SELECT
            cluster_name,
            cluster_id,
            creator_user_name,
            environment,
            autotermination_minutes,
            spark_version,
            runtime_engine,
            state,
            to_timestamp(cast(start_time/1000 as double)) as start_time,
            to_timestamp(cast(terminated_time/1000 as double)) as terminated_time,
            to_timestamp(cast(last_restarted_time/1000 as double)) as last_restarted_time,
            datediff('minute', to_timestamp(cast(last_restarted_time/1000 as double)), to_timestamp(cast(terminated_time/1000 as double))) as uptime_minutes,
            autoscale
        FROM df
    """).fetchdf()

    return df


st.title("AcrossDa")
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

            col1, col2 = st.columns(2)
            with col1:
                if st.button("List Clusters"):
                    st.info("Processing workspaces...")
                    df = process_workspaces(data)
                    if not df.empty:
                        st.success(f"Found clusters across {len(data)} workspaces")
                        final_df = select_and_convert_times(df)
                        st.dataframe(final_df,hide_index=True)
                    else:
                        st.warning("No clusters found in any workspace")
            
            with col2:
                if st.button("List Metastores"):
                    if selected_workspace:
                        st.info(f"Fetching metastore details for {selected_url}...")
                        metastore_details = get_metastore_details(selected_workspace)
                        if metastore_details:
                            st.json(metastore_details)
                        else:
                            st.warning("No metastore details found")
    
    except json.JSONDecodeError:
        st.error("Invalid JSON file")
    except Exception as e:
        st.error(f"An error occurred: {str(e)}")