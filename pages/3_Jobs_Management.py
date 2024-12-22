import streamlit as st
import json
import requests
from typing import Dict, List
import pandas as pd
from databricks_utils import make_api_request, load_workspace_config, convert_timestamp_columns
from dateutil import tz
import duckdb

st.set_page_config(layout="wide")
st.title("Jobs Management")

# Add timezone selector
timezone = st.selectbox("Select Timezone", ["UTC", "IST", "MST","PST"], index=0)

def get_jobs(workspace_info: Dict, job_id: str = None, limit: int = 0) -> List[Dict]:
    """
    Get list of job runs for a specific workspace
    """
    params = {"completed_only": "false","limit":limit}
    if job_id:
        params['job_id'] = job_id
    response_data = make_api_request(workspace_info, "/api/2.2/jobs/runs/list", params=params)
    runs = response_data.get('runs', [])
    
    # Add workspace information to each run
    for run in runs:
        run['workspace_url'] = workspace_info['url']
        run['environment'] = workspace_info['environment']
    
    return runs

def process_jobs_data(jobs_data: List[Dict]) -> pd.DataFrame:
    """
    Process jobs data into a pandas DataFrame with relevant columns using DuckDB
    """
    if not jobs_data:
        return pd.DataFrame()

    # Create DataFrame and register as DuckDB table
    df = pd.json_normalize(jobs_data)
    df.rename(columns=lambda x: x.replace('.', '_'), inplace=True)
    for col in ['state_result_state', 'status_state', 'status_termination_details_message']:
        if col not in df.columns:
            df[col] = "NA"
    
    conn = duckdb.connect()
    conn.register('jobs_temp', df)
    
    # SQL query to transform the data
    query = """
    SELECT 
        run_name as job_name,
        state_result_state,
        status_state,
        status_termination_details_message as termination_message,
        start_time,
        end_time,
        round((run_duration / 1000.0) / 60.0, 2) as run_duration_min,
        run_page_url,
        run_type,
        creator_user_name,
        job_id,
        run_id,
        workspace_url,
        environment
    FROM jobs_temp
    """
    
    # Execute query and get results
    result_df = conn.execute(query).df()
    
    # Convert timestamp columns
    timestamp_columns = ['start_time', 'end_time']
    result_df = convert_timestamp_columns(result_df, timestamp_columns, timezone)
    
    # Close connection
    conn.close()
    
    return result_df

# Main app logic
uploaded_file = st.file_uploader("Choose JSON file with Workspace details", type=["json"])

if uploaded_file is not None:
    workspaces = load_workspace_config(uploaded_file)
    
    if workspaces:
        # Create a list of URLs from the workspace configurations
        urls = [workspace['url'] for workspace in workspaces]
        selected_url = st.selectbox("Select Workspace URL", urls)
        
        # Get the selected workspace configuration
        selected_workspace = next((workspace for workspace in workspaces if workspace['url'] == selected_url), None)
        
        if selected_workspace:
            if st.button("List Job Runs"):
                with st.spinner(f"Fetching job runs from {selected_url}..."):
                    # Only get jobs for the selected workspace
                    jobs_data = get_jobs(selected_workspace)
                    jobs_df = process_jobs_data(jobs_data)
                    if not jobs_df.empty:
                        st.dataframe(jobs_df, hide_index=True, use_container_width=True)
                    else:
                        st.info("No jobs found")

            st.divider()
            job_ids = st.text_input("Enter jobID(s) separated by commas")
            if st.button("List Specific Job(s) latest status"):
                if job_ids:
                    job_ids = [int(job_id.strip()) for job_id in job_ids.split(",")]
                    jobs_data = []
                    for job_id in job_ids:
                        with st.spinner(f"Fetching job run status for jobID: {job_id} from {selected_url}..."):
                            jobs_data.extend(get_jobs(selected_workspace, job_id,limit=1))
                    jobs_df = process_jobs_data(jobs_data)
                    if not jobs_df.empty:
                        st.dataframe(jobs_df, hide_index=True, use_container_width=True)
                    else:
                        st.info(f"No jobs found for jobID(s): {', '.join(map(str, job_ids))}")