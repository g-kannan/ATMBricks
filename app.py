import streamlit as st
import json
import pandas as pd
import requests
from typing import List, Dict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import pytz
import duckdb
from databricks_utils import (
    make_api_request, process_parallel, convert_timestamp_columns,
    load_workspace_config, setup_workspace_selector
)

st.set_page_config(layout="wide",page_title='ATMBricks')
st.image("./assets/ATMBricks_Logo.png")

# Add timezone selector at the top level
timezone = st.selectbox("Select Timezone", ["UTC", "IST", "MST","PST"], index=0)

def query_clusters(workspace_info: Dict) -> List[Dict]:
    """
    Query clusters for a specific workspace
    """
    response_data = make_api_request(workspace_info, "/api/2.1/clusters/list")
    clusters = response_data.get('clusters', [])
    
    # Add workspace information to each cluster
    for cluster in clusters:
        cluster['workspace_url'] = workspace_info['url']
        cluster['environment'] = workspace_info['environment']
        
    return clusters

def process_workspaces(workspaces: List[Dict]) -> pd.DataFrame:
    """
    Process multiple workspaces in parallel and combine results
    """
    return process_parallel(workspaces, query_clusters)

def query_warehouses(workspace_info: Dict) -> List[Dict]:
    """
    Query SQL warehouses for a specific workspace
    """
    response_data = make_api_request(workspace_info, "/api/2.0/sql/warehouses")
    warehouses = response_data.get('warehouses', [])
    
    # Add workspace information to each warehouse
    for warehouse in warehouses:
        warehouse['workspace_url'] = workspace_info['url']
        warehouse['environment'] = workspace_info['environment']
        
    return warehouses

def process_clusters_data(df: pd.DataFrame,) -> pd.DataFrame:
    conn = duckdb.connect()
    conn.register('clusters_temp', df)
    # SQL query to transform the data
    query = """
    SELECT cluster_name,
            cluster_id,
            environment,
            autotermination_minutes,
    datediff('minute', to_timestamp(cast(last_restarted_time/1000 as double)), to_timestamp(cast(terminated_time/1000 as double))) as usage_minutes,
    case when terminated_time is null then datediff('minute', to_timestamp(cast(last_restarted_time/1000 as double)), current_timestamp) else 0 end as uptime_minutes,
    spark_version,
    runtime_engine,cluster_source,creator_user_name,
    state,state_message,start_time, terminated_time, last_restarted_time,workspace_url
    FROM clusters_temp
    """
    
    # Execute query and get results
    processed_df = conn.execute(query).df()
    timestamp_columns = ['start_time', 'terminated_time', 'last_restarted_time']
    result_df = convert_timestamp_columns(processed_df, timestamp_columns, timezone)
    conn.close()
    return result_df

def process_warehouses(workspaces: List[Dict]) -> pd.DataFrame:
    """
    Process multiple workspaces in parallel and combine warehouse results
    """
    return process_parallel(workspaces, query_warehouses)

def highlight_high_usage(value,threshold=10):
    return ['background-color: red' if isinstance(v, (int, float)) and v > threshold else '' for v in value]

uploaded_file = st.file_uploader("Choose JSON file with Workspace details", type=["json"])

show_sample_json = st.checkbox("Show Sample JSON")
if show_sample_json:
    st.json("""
    [
        {
        "url": "https://workspace1.clouddatabricks.net",
        "token": "token",
        "environment": "prod"
        },
        {
        "url": "https://workspace2.clouddatabricks.net",
        "token": "token",
        "environment": "prod",
        "jobs": {
            "etl": "680318963389412,887764713478314",
            "dashboards": "680318963389412,887764713478314"
            }
        }
    ]
    """)

high_usage_threshold = st.number_input("Highlight clusters with usage > Mins", min_value=1, max_value=1000, value=10)

if uploaded_file is not None:
    try:
        data = json.load(uploaded_file)
        
        if not isinstance(data, list):
            st.error("Invalid JSON format. Expected a list of workspace configurations.")
        else:
            if st.button("List Clusters",icon=":material/host:"):
                st.info("Processing workspaces...")
                df = process_workspaces(data)
                if not df.empty:
                    st.success(f"Found clusters across {len(data)} workspaces")
                    final_df = process_clusters_data(df)
                    final_df = final_df[final_df['cluster_source'] != 'JOB']
                    st.dataframe(final_df.style.apply(highlight_high_usage,threshold=high_usage_threshold),hide_index=True)
                else:
                    st.warning("No clusters found in any workspace")
            
            if st.button("List Warehouses",icon=":material/warehouse:"):
                st.info("Processing workspaces...")
                df = process_warehouses(data)
                if not df.empty:
                    st.success(f"Found warehouses across {len(data)} workspaces")
                    st.dataframe(df, hide_index=True)
                else:
                    st.warning("No warehouses found in any workspace")
                
    except json.JSONDecodeError:
        st.error("Invalid JSON file")
    except Exception as e:
        st.error(f"An error occurred: {str(e)}")