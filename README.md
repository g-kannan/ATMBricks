![ATMBricks](assets/ATMBricks.png)

ATMBricks (Audit, Tweak & Make Databricks API Calls easily) is a Streamlit-based application that simplifies the management and monitoring of Databricks workspaces.

## Features

### 1. Cluster Management (Main Dashboard)
- **Cluster Monitoring**: View and track all clusters across multiple workspaces
- **Detailed Information**: Access key cluster metrics including:
  - Cluster names and IDs
  - Creator information
  - Environment details
  - Auto-termination settings
  - Spark versions
  - Runtime engine details
  - Cluster states
  - Start/termination times
  - Usage metrics
- **Multi-workspace Support**: Process and view clusters from multiple workspaces simultaneously
- **Parallel Processing**: Efficient data gathering using concurrent API calls

### 2. Admin Tools
- **Metastore Management**:
  - List and view metastore details
  - Get system schema status
  - Enable system schemas
- **Workspace Selection**: Easy switching between different workspace configurations
- **System Schema Management**: Enable and configure system schemas with simple UI controls

### 3. Secret Management (Coming Soon)
- Work in progress feature for managing Databricks secrets

## Ease of Use

1. **Simple Configuration**:
   - Upload workspace details via JSON file
   - Sample JSON format provided for quick setup

2. **User-Friendly Interface**:
   - Clean, wide-layout design
   - Dropdown menus for workspace selection
   - Interactive buttons for key functions
   - Clear success/error messages

3. **Data Visualization**:
   - Organized data tables
   - Easy-to-read DataFrame displays
   - UTC time conversions for consistency

## Getting Started

1. Prepare a JSON file with your workspace details
2. Upload the file using the file uploader
3. Select your workspace from the dropdown
4. Use the various features through the intuitive UI

## Run Locally

1. Clone the repo: `git clone https://github.com/g-kannan/ATMBricks.git`
2. Install dependencies: `pip install -r requirements.txt`
3. Run the app: `streamlit run app.py`

## Contribute
Please raise an issue if you encounter any bugs or have any suggestions: https://github.com/g-kannan/ATMBricks/issues
