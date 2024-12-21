import streamlit as st
import requests
import pandas as pd
from typing import Dict, List, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
import duckdb
import json

def make_api_request(workspace_info: Dict, endpoint: str, method: str = "GET", data: Optional[Dict] = None) -> Dict:
    """
    Make an API request to Databricks workspace
    """
    url = f"{workspace_info['url']}{endpoint}"
    headers = {
        "Authorization": f"Bearer {workspace_info['token']}",
        "Content-Type": "application/json"
    }
    
    try:
        if method == "GET":
            response = requests.get(url, headers=headers)
        elif method == "PUT":
            response = requests.put(url, headers=headers, json=data)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        st.error(f"Error accessing {url}: {str(e)}")
        return {}

def process_parallel(workspaces: List[Dict], query_func) -> pd.DataFrame:
    """
    Process multiple workspaces in parallel and combine results
    """
    all_results = []
    
    with ThreadPoolExecutor(max_workers=10) as executor:
        future_to_workspace = {
            executor.submit(query_func, workspace): workspace 
            for workspace in workspaces
        }
        
        for future in as_completed(future_to_workspace):
            results = future.result()
            all_results.extend(results)
    
    if not all_results:
        return pd.DataFrame()
    
    return pd.DataFrame(all_results)

def convert_timestamp_columns(df: pd.DataFrame, timestamp_columns: List[str], timezone: str = 'UTC') -> pd.DataFrame:
    """
    Convert Unix timestamps to datetime with specified timezone
    Args:
        df: Input DataFrame
        timestamp_columns: List of column names containing timestamps
        timezone: Target timezone (UTC/IST/MST)
    """
    conn = duckdb.connect()
    
    # Build the SQL query dynamically based on the timestamp columns
    select_parts = []
    for col in df.columns:
        if col in timestamp_columns:
            select_parts.append(f"to_timestamp(cast({col}/1000 as double)) AT TIME ZONE '{timezone}' as {col}")
        else:
            select_parts.append(col)
    
    sql_query = f"SELECT {', '.join(select_parts)} FROM df"
    return conn.execute(sql_query).fetchdf()

def load_workspace_config(uploaded_file) -> List[Dict]:
    """
    Load and validate workspace configuration from uploaded JSON file
    """
    try:
        data = json.load(uploaded_file)
        if not isinstance(data, list):
            st.error("Invalid JSON format. Expected a list of workspace configurations.")
            return []
        return data
    except json.JSONDecodeError:
        st.error("Invalid JSON file")
        return []
    except Exception as e:
        st.error(f"An error occurred: {str(e)}")
        return []

def setup_workspace_selector(workspaces: List[Dict]) -> Optional[Dict]:
    """
    Create a workspace selector dropdown and return selected workspace
    """
    urls = [workspace['url'] for workspace in workspaces]
    selected_url = st.selectbox("Select Workspace URL", urls)
    return next((workspace for workspace in workspaces if workspace['url'] == selected_url), None)
