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
            datediff('minute', to_timestamp(cast(last_restarted_time/1000 as double)), to_timestamp(cast(terminated_time/1000 as double))) as usage_minutes,
            autoscale
        FROM df
    """).fetchdf()

    return df

def query_warehouses(workspace_info: Dict) -> List[Dict]:
    """
    Query SQL warehouses for a specific workspace
    """
    url = f"{workspace_info['url']}/api/2.0/sql/warehouses"
    headers = {
        "Authorization": f"Bearer {workspace_info['token']}",
        "Content-Type": "application/json"
    }
    
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        warehouses = response.json().get('warehouses', [])
        
        # Add workspace information to each warehouse
        for warehouse in warehouses:
            warehouse['workspace_url'] = workspace_info['url']
            warehouse['environment'] = workspace_info['environment']
            
        return warehouses
    except Exception as e:
        st.error(f"Error querying warehouses for {workspace_info['url']}: {str(e)}")
        return []

def process_warehouses(workspaces: List[Dict]) -> pd.DataFrame:
    """
    Process multiple workspaces in parallel and combine warehouse results
    """
    all_warehouses = []
    
    with ThreadPoolExecutor(max_workers=10) as executor:
        future_to_workspace = {
            executor.submit(query_warehouses, workspace): workspace 
            for workspace in workspaces
        }
        
        for future in as_completed(future_to_workspace):
            warehouses = future.result()
            all_warehouses.extend(warehouses)
    
    if not all_warehouses:
        return pd.DataFrame()
    
    return pd.DataFrame(all_warehouses)

st.title("AcrossDa")
uploaded_file = st.file_uploader("Choose a JSON file", type=["json"])

if uploaded_file is not None:
    try:
        data = json.load(uploaded_file)
        
        if not isinstance(data, list):
            st.error("Invalid JSON format. Expected a list of workspace configurations.")
        else:
            if st.button("List Clusters"):
                st.info("Processing workspaces...")
                df = process_workspaces(data)
                if not df.empty:
                    st.success(f"Found clusters across {len(data)} workspaces")
                    final_df = select_and_convert_times(df)
                    st.dataframe(final_df,hide_index=True)
                else:
                    st.warning("No clusters found in any workspace")
            
            if st.button("List Warehouses"):
                st.info("Processing workspaces...")
                warehouses_df = process_warehouses(data)
                if not warehouses_df.empty:
                    st.success(f"Found warehouses across {len(data)} workspaces")
                    st.dataframe(warehouses_df,hide_index=True)
                else:
                    st.warning("No warehouses found in any workspace")
                
    except json.JSONDecodeError:
        st.error("Invalid JSON file")
    except Exception as e:
        st.error(f"An error occurred: {str(e)}")