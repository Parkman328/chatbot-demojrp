# Standard library imports
import json
import os
import time
import tomllib
from typing import Dict, List, Optional, Tuple

# Third-party imports
import pandas as pd
import requests
import streamlit as st
from PIL import Image

# Snowflake related imports
import snowflake.connector
from snowflake.connector.errors import DatabaseError
from snowflake.snowpark import Session
from snowflake.snowpark.exceptions import SnowparkSQLException

# Initialize session as None at the global scope
session = None

# Check if we're in debug mode
# In development, set DEBUG=true in environment variables
DEBUG_MODE = os.environ.get("DEBUG", "false").lower() == "true"

# Initialize connection parameters in session state if they don't exist
def init_connection_params():
    # Initialize connection parameters with default values
    if "snowflake_account" not in st.session_state:
        st.session_state.snowflake_account = ""
    if "snowflake_user" not in st.session_state:
        st.session_state.snowflake_user = ""
    if "snowflake_authenticator" not in st.session_state:
        st.session_state.snowflake_authenticator = 'snowflake'
    if "snowflake_token" not in st.session_state:
        st.session_state.snowflake_token = ""
    if "snowflake_warehouse" not in st.session_state:
        st.session_state.snowflake_warehouse = ""
    if "snowflake_database" not in st.session_state:
        st.session_state.snowflake_database = ""
    if "snowflake_schema" not in st.session_state:
        st.session_state.snowflake_schema = ""
    
    # Connection details - use st.secrets only in debug mode
    if DEBUG_MODE and "connections" in st.secrets and "snowflake" in st.secrets["connections"]:
        # Use secrets in debug mode
        secrets = st.secrets["connections"]["snowflake"]
        st.session_state.snowflake_account = secrets["ACCOUNT"]
        st.session_state.snowflake_user = secrets["USER"]
        st.session_state.snowflake_authenticator = 'snowflake'
        st.session_state.snowflake_token = secrets["PAT"]
        st.session_state.snowflake_warehouse = secrets["WAREHOUSE"]
        st.session_state.snowflake_database = secrets["DATABASE"]
        st.session_state.snowflake_schema = secrets["SCHEMA"]
    else:
        # Use environment variables if available
        env_account = os.environ.get("SNOWFLAKE_ACCOUNT", "")
        env_user = os.environ.get("SNOWFLAKE_USER", "")
        env_token = os.environ.get("SNOWFLAKE_PAT", "")
        env_warehouse = os.environ.get("SNOWFLAKE_WAREHOUSE", "")
        env_database = os.environ.get("SNOWFLAKE_DATABASE", "")
        env_schema = os.environ.get("SNOWFLAKE_SCHEMA", "")
        
        # Only update if environment variables are set
        if env_account:
            st.session_state.snowflake_account = env_account
        if env_user:
            st.session_state.snowflake_user = env_user
        if env_token:
            st.session_state.snowflake_token = env_token
        if env_warehouse:
            st.session_state.snowflake_warehouse = env_warehouse
        if env_database:
            st.session_state.snowflake_database = env_database
        if env_schema:
            st.session_state.snowflake_schema = env_schema

# List of available semantic model paths in the format: <DATABASE>.<SCHEMA>.<STAGE>/<FILE-NAME>
# Each path points to a YAML file defining a semantic model

AVAILABLE_SEMANTIC_MODELS_PATHS = [
    "CORTEX_DEMOS.CONTOSO.ANALYST_STAGE/ContosoDemo.yaml",
    "CORTEX_ANALYST_DEMO.REVENUE_TIMESERIES.RAW_DATA/revenue_timeseries.yaml"
]
API_ENDPOINT = "/api/v2/cortex/analyst/message"
FEEDBACK_API_ENDPOINT = "/api/v2/cortex/analyst/feedback"
API_TIMEOUT = 50000  # in milliseconds


def main():
    """
    Main function to initialize and run the Streamlit application.
    """
    # Initialize session state
    if "messages" not in st.session_state:
        reset_session_state()
    
    # Initialize connection parameters
    init_connection_params()
    
    show_header_and_sidebar()
    
    # Check if we have an active Snowflake connection
    has_connection = (
        "CONN" in st.session_state
        and st.session_state.CONN is not None
        and hasattr(st.session_state.CONN, "rest")
        and st.session_state.CONN.rest is not None
        and hasattr(st.session_state.CONN.rest, "token")
        and st.session_state.CONN.rest.token is not None
    )
    
    # Only initiate the first question if we have a connection and no messages yet
    if len(st.session_state.messages) == 0 and has_connection:
        process_user_input("What questions can I ask?")
    elif len(st.session_state.messages) == 0 and not has_connection:
        # Add a welcome message instructing to connect first
        welcome_message = {
            "role": "analyst",
            "content": [{
                "type": "text", 
                "text": "👋 Welcome to Snowflake Cortext Analyst for Qlik Cloud! Please connect to Snowflake using the sidebar before asking questions."
            }],
            "request_id": "welcome"
        }
        st.session_state.messages.append(welcome_message)
    
    display_conversation()
    handle_user_inputs()
    handle_error_notifications()



def reset_session_state():
    """Reset important session state elements while preserving connection information."""
    # Preserve Snowflake connection if it exists
    snowflake_conn = st.session_state.get('CONN')
    snowflake_session = st.session_state.get('session')
    
    # Reset conversation state
    st.session_state.messages = []  # List to store conversation messages
    st.session_state.active_suggestion = None  # Currently selected suggestion
    
    # Restore Snowflake connection if it existed
    if snowflake_conn is not None:
        st.session_state.CONN = snowflake_conn
    if snowflake_session is not None:
        st.session_state.session = snowflake_session


def show_header_and_sidebar():
    """Display the header and sidebar of the app."""
    # Page config
    st.set_page_config(page_title="Snowflake Cortext Analyst", layout="wide")
    with open("./config/config_readme.toml", "rb") as f:
        readme = tomllib.load(f)
    # Show title and description.
    st.markdown(
        "<span style='font-size:2em; font-weight:bold;'>Snowflake Cortext Analyst for Qlik Cloud</span>",
        unsafe_allow_html=True
    )
    with st.expander(
        "Snowflake Cortext Analyst for Qlik Cloud Instructions", expanded=False
        ):
        st.markdown(
            readme['app']['app_intro']
        )
        st.write("")
        st.write("")
    # Display logo at the top of the sidebar
    logo_path = "./references/qlik_snowflake_cortext.png"
    if os.path.exists(logo_path):
        st.sidebar.image(Image.open(logo_path), use_container_width=True)
    else:
        st.sidebar.write(":warning: Logo not found.")

    # Sidebar with a reset button
    with st.sidebar:
        col1, col2 = st.columns(2)
        with col1:
            st.markdown(f"[Github Repository]({readme['links']['repo']})")
        with col2:
            st.markdown(f"[Article]({readme['links']['article']})")
        st.divider()
        st.sidebar.title("1. Configuration")
        with st.sidebar.expander("Configurations", expanded=not DEBUG_MODE):
            st.markdown("## Snowflake Connection Details")
            # Allow editing of fields when not in debug mode
            st.text_input("Account", value=st.session_state.snowflake_account, 
                         disabled=DEBUG_MODE, key="snowflake_account")
            st.text_input("User", value=st.session_state.snowflake_user, 
                         disabled=DEBUG_MODE, key="snowflake_user")
            st.text_input("Personal Access Token", value=st.session_state.snowflake_token, 
                         type="password", disabled=DEBUG_MODE, key="snowflake_token")
            st.text_input("Warehouse", value=st.session_state.snowflake_warehouse, 
                         disabled=DEBUG_MODE, key="snowflake_warehouse")
            st.text_input("Database", value=st.session_state.snowflake_database, 
                         disabled=DEBUG_MODE, key="snowflake_database")
            st.text_input("Schema", value=st.session_state.snowflake_schema, 
                         disabled=DEBUG_MODE, key="snowflake_schema")
            
        st.sidebar.title("2. Connect to Snowflake")
        with st.sidebar.expander("Connect", expanded=False):
            connect_button = st.button("Connect to Snowflake")
        if connect_button:
            # Get values from session state
            account = st.session_state.snowflake_account
            user = st.session_state.snowflake_user
            token = st.session_state.snowflake_token
            warehouse = st.session_state.snowflake_warehouse
            database = st.session_state.snowflake_database
            schema = st.session_state.snowflake_schema
                
            if not token:
                st.error("Please provide a valid Personal Access Token.")
            elif not account:
                st.error("Please provide a valid Account.")
            elif not user:
                st.error("Please provide a valid User.")
            else:
                try:
                    conn = snowflake.connector.connect(
                        account=account,
                        user=user,
                        authenticator='snowflake',
                        password=token,
                        warehouse=warehouse,
                        database=database,
                        schema=schema
                    )
                    st.session_state.CONN = conn
                    st.success(f"Connection successful to {account}!")
                    connection_parameters = {
                        "account": account,
                        "user": user,
                        "authenticator": 'snowflake',
                        "password": token,
                        "warehouse": warehouse,
                        "database": database,
                        "schema": schema,
                    }
                    # Initialize the global session variable
                    global session
                    session = Session.builder.configs(connection_parameters).create()
                    st.session_state.session = session  # Also store in session state for persistence
                    
                    cur = conn.cursor()
                    #cur.execute("SELECT CURRENT_VERSION(), CURRENT_WAREHOUSE(), CURRENT_DATABASE(), CURRENT_SCHEMA()")
                    #result = cur.fetchall()
                    #st.write("Connection Info:")
                    #for row in result:
                        #st.write(row)
                    cur.close()
                except DatabaseError as e:
                    st.error(f"Failed to connect: {e}")
                    _, btn_container, _ = st.columns([2, 6, 2])
        st.sidebar.title("3. Semantic Model")
        with st.sidebar.expander("Select Semantic Model", expanded=False):
            #st.markdown("Select the semantic model to use for the Analyst API.")
            #st.markdown("You can upload your own semantic model YAML files to the specified stage in Snowflake and add the path here.")
            #st.markdown("Make sure the semantic model is compatible with the Analyst API.")
            st.selectbox("", AVAILABLE_SEMANTIC_MODELS_PATHS,
                format_func=lambda s: s.split("/")[-1],
                key="selected_semantic_model_path",
                on_change=reset_session_state,
            )
        st.sidebar.title("4. Clean Up Session")
        with st.sidebar.expander("Reset Session", expanded=False):
            if st.button("Clear Chat History", use_container_width=True):
                reset_session_state()       
        st.divider()


def handle_user_inputs():
    """
    Handle user inputs from the chat interface and suggested questions.
    """
    # Handle chat input
    user_input = st.chat_input("What is your question?")
    if user_input:
        process_user_input(user_input)
    # Handle suggested question click
    elif st.session_state.active_suggestion is not None:
        suggestion = st.session_state.active_suggestion
        st.session_state.active_suggestion = None
        process_user_input(suggestion)

def handle_error_notifications():
    """Display error notifications using toast messages."""
    if st.session_state.get("fire_API_error_notify"):
        st.toast("An API error has occurred!", icon="🚨")
        st.session_state["fire_API_error_notify"] = False

def process_user_input(prompt: str):
    """
    Process user input and update the conversation history.

    Args:
        prompt (str): The user's input.
    """

    # Create a new message, append to history and display imidiately
    new_user_message = {
        "role": "user",
        "content": [{"type": "text", "text": prompt}],
    }
    st.session_state.messages.append(new_user_message)
    with st.chat_message("user"):
        user_msg_index = len(st.session_state.messages) - 1
        display_message(new_user_message["content"], user_msg_index)

    # Show progress indicator inside analyst chat message while waiting for response
    with st.chat_message("analyst"):
        with st.spinner("Waiting for Analyst's response..."):
            time.sleep(1)
            try:
                response, error_msg = get_analyst_response(st.session_state.messages)
                
                if error_msg is None and response and "message" in response and "content" in response["message"]:
                    analyst_message = {
                        "role": "analyst",
                        "content": response["message"]["content"],
                        "request_id": response.get("request_id", "unknown"),
                    }
                else:
                    # Handle error or malformed response
                    request_id = response.get("request_id", "unknown") if response else "unknown"
                    error_text = error_msg or "Received an invalid response format from the Analyst API."
                    
                    analyst_message = {
                        "role": "analyst",
                        "content": [{"type": "text", "text": error_text}],
                        "request_id": request_id,
                    }
                    st.session_state["fire_API_error_notify"] = True
                    
                st.session_state.messages.append(analyst_message)
                st.rerun()
            except Exception as e:
                # Handle unexpected exceptions
                error_text = f"An unexpected error occurred: {str(e)}"
                analyst_message = {
                    "role": "analyst",
                    "content": [{"type": "text", "text": error_text}],
                    "request_id": "error",
                }
                st.session_state["fire_API_error_notify"] = True
                st.session_state.messages.append(analyst_message)
                st.rerun()

def get_analyst_response(messages: List[Dict]) -> Tuple[Dict, Optional[str]]:
    """
    Send chat history to the Cortex Analyst API and return the response.

    Args:
        messages (List[Dict]): The conversation history.

    Returns:
        Tuple[Dict, Optional[str]]: The response from the Cortex Analyst API and any error message.
    """
    account = st.session_state.snowflake_account
    HOST = account + ".snowflakecomputing.com"
    print(f"Using semantic model: {st.session_state.selected_semantic_model_path}")
    
    # Prepare the request body with the user's prompt
    request_body = {
        "messages": messages,
        "semantic_model_file": f"@{st.session_state.selected_semantic_model_path}",
    }

    # Check if connection and token are available
    if (
        "CONN" not in st.session_state
        or st.session_state.CONN is None
        or not hasattr(st.session_state.CONN, "rest")
        or st.session_state.CONN.rest is None
        or not hasattr(st.session_state.CONN.rest, "token")
        or st.session_state.CONN.rest.token is None
    ):
        error_msg = "❌ Not connected to Snowflake. Please connect first using the sidebar."
        return {"request_id": None}, error_msg

    try:
        # Send a POST request to the Cortex Analyst API endpoint
        resp = requests.post(
            url=f"https://{HOST}{API_ENDPOINT}",
            json=request_body,
            headers={
                "Authorization": f'Snowflake Token="{st.session_state.CONN.rest.token}"',
                "Content-Type": "application/json",
            },
            timeout=API_TIMEOUT/1000,  # Convert milliseconds to seconds
        )
        request_id = resp.headers.get("X-Snowflake-Request-Id", "unknown")
        
        if resp.status_code < 400:
            response_json = resp.json()
            # Validate response structure
            if "message" in response_json and "content" in response_json["message"]:
                return {**response_json, "request_id": request_id}, None
            else:
                error_msg = "Received an invalid response format from the Analyst API."
                return {"request_id": request_id}, error_msg
        else:
            # Craft readable error message for HTTP errors
            try:
                parsed_content = resp.json()
                error_msg = f"""
🚨 An Analyst API error has occurred 🚨

* response code: `{resp.status_code}`
* request-id: `{request_id}`
* error code: `{parsed_content.get('error_code', 'N/A')}`

Message:
```
{parsed_content.get('message', 'No message provided')}
```
                """
            except:
                error_msg = f"Failed request (id: {request_id}) with status {resp.status_code}: {resp.content}"
            
            return {"request_id": request_id}, error_msg
            
    except requests.exceptions.Timeout:
        error_msg = f"Request timed out after {API_TIMEOUT/1000} seconds. Please try again."
        return {"request_id": "timeout"}, error_msg
    except requests.exceptions.RequestException as e:
        error_msg = f"Request error: {str(e)}"
        return {"request_id": "error"}, error_msg

def display_conversation():
    """
    Display the conversation history between the user and the assistant.
    """
    for idx, message in enumerate(st.session_state.messages):
        role = message["role"]
        content = message["content"]
        
        # Use snowflake emoji as avatar for analyst role
        if role == "analyst":
            with st.chat_message(role, avatar="❄️"):
                display_message(content, idx)
        else:
            with st.chat_message(role):
                display_message(content, idx)

def display_message(content: List[Dict[str, str]], message_index: int):
    """
    Display a single message content with various content types.

    Args:
        content (List[Dict[str, str]]): The message content.
        message_index (int): The index of the message for unique component keys.
    """
    if not content:
        st.warning("Empty message content")
        return
        
    for item_index, item in enumerate(content):
        if not isinstance(item, dict) or "type" not in item:
            st.warning(f"Invalid message item format: {item}")
            continue
            
        item_type = item.get("type")
        
        if item_type == "text":
            if "text" in item:
                st.markdown(item["text"])
            else:
                st.warning("Text item missing 'text' field")
                
        elif item_type == "suggestions":
            # Display suggestions as buttons
            if "suggestions" in item and isinstance(item["suggestions"], list):
                for suggestion_index, suggestion in enumerate(item["suggestions"]):
                    if st.button(
                        suggestion, key=f"suggestion_{message_index}_{item_index}_{suggestion_index}"
                    ):
                        st.session_state.active_suggestion = suggestion
            else:
                st.warning("Suggestions item missing valid 'suggestions' list")
                
        elif item_type == "sql":
            # Display the SQL query and results
            if "statement" in item:
                display_sql_query(item["statement"], message_index)
            else:
                st.warning("SQL item missing 'statement' field")
                
        elif item_type == "error":
            # Display error messages with a distinctive style
            if "text" in item:
                st.error(item["text"])
            else:
                st.error("Unknown error occurred")
                
        else:
            # Handle other content types
            st.info(f"Unsupported content type: {item_type}")
            st.json(item)

@st.cache_data(show_spinner=False)
def get_query_exec_result(query: str) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
    """
    Execute the SQL query and convert the results to a pandas DataFrame.

    Args:
        query (str): The SQL query to execute.

    Returns:
        Tuple[Optional[pd.DataFrame], Optional[str]]: The query results as DataFrame and any error message.
    """
    # Use session from session state if available, otherwise use global session
    current_session = st.session_state.get('session') or session
    
    if current_session is None:
        return None, "No active Snowflake session. Please connect to Snowflake first."
        
    try:
        df = current_session.sql(query).to_pandas()
        return df, None
    except SnowparkSQLException as e:
        return None, str(e)


def display_sql_query(sql: str, message_index: int):
    """
    Executes the SQL query and displays the results in form of dataframe and charts.

    Args:
        sql (str): The SQL query to execute.
        message_index (int): The index of the message for unique component keys.
    """

    # Display the SQL query
    with st.expander("SQL Query", expanded=False):
        st.code(sql, language="sql")

    # Check if we have an active Snowflake session
    current_session = st.session_state.get('session') or session
    if current_session is None:
        with st.expander("Results", expanded=True):
            st.error("No active Snowflake session. Please connect to Snowflake using the sidebar first.")
            return

    # Display the results of the SQL query
    with st.expander("Results", expanded=True):
        with st.spinner("Running SQL..."):
            df, err_msg = get_query_exec_result(sql)
            if df is None:
                st.error(f"Could not execute generated SQL query. Error: {err_msg}")
                return

            if df.empty:
                st.write("Query returned no data")
                return
            # Generate insights using Snowflake Cortex if the result set is small enough
            if len(df) <= 20:
                sql_new = f'''SELECT SNOWFLAKE.CORTEX.COMPLETE(
                    'mistral-large2',
                    'Summarize results, Show trends & Itemize top insights & trends from the following json data in less than 150 words. Data: ' ||
                    (
                        SELECT array_agg(object_construct(*))::string as Output from
                        ({sql.replace(";", "")})
                    )
                ) as Insights'''
                
                DataSummary = get_query_exec_result(sql_new)
                st.markdown(str(DataSummary[0].iat[0, 0]))
            
            # Show query results in two tabs
            data_tab, chart_tab = st.tabs(["Data 📄", "Chart 📈 "])
            with data_tab:
                st.dataframe(df, use_container_width=True)

            with chart_tab:
                display_charts_tab(df, message_index)


def display_charts_tab(df: pd.DataFrame, message_index: int) -> None:
    """
    Display the charts tab.

    Args:
        df (pd.DataFrame): The query results.
        message_index (int): The index of the message.
    """
    # There should be at least 2 columns to draw charts
    if len(df.columns) >= 2:
        all_cols_set = set(df.columns)
        col1, col2 = st.columns(2)
        x_col = col1.selectbox(
            "X axis", all_cols_set, key=f"x_col_select_{message_index}"
        )
        y_col = col2.selectbox(
            "Y axis",
            all_cols_set.difference({x_col}),
            key=f"y_col_select_{message_index}",
        )
        chart_type = st.selectbox(
            "Select chart type",
            options=["Line Chart 📈", "Bar Chart 📊"],
            key=f"chart_type_{message_index}",
        )
        if chart_type == "Line Chart 📈":
            st.line_chart(df.set_index(x_col)[y_col])
        elif chart_type == "Bar Chart 📊":
            st.bar_chart(df.set_index(x_col)[y_col])
    else:
        st.write("At least 2 columns are required")


if __name__ == "__main__":
    main()
    st.markdown("---")
    st.markdown("Developed by [John Park, Qlik Analytics PreSales Architect](https://www.linkedin.com/in/jpark328/) | [Email](mailto:john.park@qlik.com) | [GitHub Repo](https://github.com/Parkman328/chatbot-demojrp/)")
    st.markdown(
        "<hr style='margin-top:2em; margin-bottom:1em;'>"
        "<div style='text-align:center; color:gray;'>"
        "Developed for Demo Purpose Not Production Use."
        "<br>"
        "© 2025 Qlik, Inc. All rights reserved."
        "</div>",
        unsafe_allow_html=True
    )
