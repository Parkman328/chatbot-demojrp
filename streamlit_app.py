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
    
    # Check if input values exist in session state (these come from input boxes)
    has_input_values = (
        "account_input" in st.session_state or
        "user_input" in st.session_state or
        "token_input" in st.session_state or
        "warehouse_input" in st.session_state or
        "database_input" in st.session_state or
        "schema_input" in st.session_state
    )
    
    # If we have input values, they take precedence over everything else
    if has_input_values:
        print("\n==== Using Input Box Values ====")
        if "account_input" in st.session_state:
            st.session_state.snowflake_account = st.session_state.account_input
            print(f"Using input box account: {st.session_state.account_input}")
            
        if "user_input" in st.session_state:
            st.session_state.snowflake_user = st.session_state.user_input
            print(f"Using input box user: {st.session_state.user_input}")
            
        if "token_input" in st.session_state:
            st.session_state.snowflake_token = st.session_state.token_input
            print(f"Using input box token (length: {len(st.session_state.token_input) if st.session_state.token_input else 0})")
            
        if "warehouse_input" in st.session_state:
            st.session_state.snowflake_warehouse = st.session_state.warehouse_input
            print(f"Using input box warehouse: {st.session_state.warehouse_input}")
            
        if "database_input" in st.session_state:
            st.session_state.snowflake_database = st.session_state.database_input
            print(f"Using input box database: {st.session_state.database_input}")
            
        if "schema_input" in st.session_state:
            st.session_state.snowflake_schema = st.session_state.schema_input
            print(f"Using input box schema: {st.session_state.schema_input}")
        return
    
    # Only use environment variables for initial values if no input box values exist
    # First priority: Environment variables (for container deployment)
    env_account = os.environ.get("SNOWFLAKE_ACCOUNT", "")
    env_user = os.environ.get("SNOWFLAKE_USER", "")
    env_token = os.environ.get("SNOWFLAKE_PAT", "")
    env_warehouse = os.environ.get("SNOWFLAKE_WAREHOUSE", "")
    env_database = os.environ.get("SNOWFLAKE_DATABASE", "")
    env_schema = os.environ.get("SNOWFLAKE_SCHEMA", "")
    env_authenticator = os.environ.get("SNOWFLAKE_AUTHENTICATOR", "snowflake")
    
    # Check if we're running in Docker
    running_in_docker = "SNOWFLAKE_ACCOUNT" in os.environ
    if running_in_docker:
        print("\n==== Docker Environment Detected (Initial Values Only) ====")
        print(f"Docker env account: {env_account}")
        print(f"Docker env user: {env_user}")
        print(f"Docker env token length: {len(env_token) if env_token else 0}")
        
        # Only set initial values from environment if session state is empty
        if not st.session_state.snowflake_account and env_account and env_account != "your-account":
            st.session_state.snowflake_account = env_account
            print(f"Setting initial account to: {env_account}")
            
        if not st.session_state.snowflake_user and env_user and env_user != "your-user":
            st.session_state.snowflake_user = env_user
            print(f"Setting initial user to: {env_user}")
            
        if not st.session_state.snowflake_token and env_token and env_token != "your-token":
            st.session_state.snowflake_token = env_token
            print(f"Setting initial token (length: {len(env_token)})")
            
        if not st.session_state.snowflake_warehouse and env_warehouse and env_warehouse != "your-warehouse":
            st.session_state.snowflake_warehouse = env_warehouse
            print(f"Setting initial warehouse to: {env_warehouse}")
            
        if not st.session_state.snowflake_database and env_database and env_database != "your-database":
            st.session_state.snowflake_database = env_database
            print(f"Setting initial database to: {env_database}")
            
        if not st.session_state.snowflake_schema and env_schema and env_schema != "your-schema":
            st.session_state.snowflake_schema = env_schema
            print(f"Setting initial schema to: {env_schema}")
            
        print("NOTE: These values will be overridden by input box values when provided.")
    
    # Print current session state values
    print("\n==== Current Session State Values ====")
    print(f"Session state account: {st.session_state.snowflake_account}")
    print(f"Session state user: {st.session_state.snowflake_user}")
    print(f"Session state token length: {len(st.session_state.snowflake_token) if st.session_state.snowflake_token else 0}")
    print(f"Session state warehouse: {st.session_state.snowflake_warehouse}")
    print(f"Session state database: {st.session_state.snowflake_database}")
    print(f"Session state schema: {st.session_state.snowflake_schema}")
        
    # Second priority: Streamlit secrets (for local development)
    if DEBUG_MODE and "connections" in st.secrets and "snowflake" in st.secrets["connections"]:
        # Only use secrets if values aren't already set by environment variables
        secrets = st.secrets["connections"]["snowflake"]
        if not st.session_state.snowflake_account and "ACCOUNT" in secrets:
            st.session_state.snowflake_account = secrets["ACCOUNT"]
        if not st.session_state.snowflake_user and "USER" in secrets:
            st.session_state.snowflake_user = secrets["USER"]
        if not st.session_state.snowflake_token and "PAT" in secrets:
            st.session_state.snowflake_token = secrets["PAT"]
        if not st.session_state.snowflake_warehouse and "WAREHOUSE" in secrets:
            st.session_state.snowflake_warehouse = secrets["WAREHOUSE"]
        if not st.session_state.snowflake_database and "DATABASE" in secrets:
            st.session_state.snowflake_database = secrets["DATABASE"]
        if not st.session_state.snowflake_schema and "SCHEMA" in secrets:
            st.session_state.snowflake_schema = secrets["SCHEMA"]

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
    # and we didn't just reset the chat history
    just_reset = st.session_state.get('just_reset', False)
    
    if len(st.session_state.messages) == 0 and has_connection and not just_reset:
        process_user_input("What questions can I ask?")
    elif len(st.session_state.messages) == 0 and not has_connection:
        # Clear the reset flag if it exists
        if just_reset:
            st.session_state.just_reset = False
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
    
    # Add a flag to prevent automatic question
    st.session_state.just_reset = True
    
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
            
            # Define callback functions to handle input changes
            def update_account():
                st.session_state.snowflake_account = st.session_state.account_input
                print(f"Updated account: {st.session_state.snowflake_account}")
            def update_user():
                st.session_state.snowflake_user = st.session_state.user_input
                print(f"Updated user: {st.session_state.snowflake_user}")
            def update_token():
                st.session_state.snowflake_token = st.session_state.token_input
                print(f"Updated token: {st.session_state.snowflake_token}")
            def update_warehouse():
                st.session_state.snowflake_warehouse = st.session_state.warehouse_input
                print(f"Updated warehouse: {st.session_state.snowflake_warehouse}")
            def update_database():
                st.session_state.snowflake_database = st.session_state.database_input
                print(f"Updated database: {st.session_state.snowflake_database}")
            def update_schema():
                st.session_state.snowflake_schema = st.session_state.schema_input
                print(f"Updated schema: {st.session_state.snowflake_schema}")
            
            # Allow editing of fields when not in debug mode
            st.text_input("Account", value=st.session_state.snowflake_account, 
                         disabled=DEBUG_MODE, key="account_input", on_change=update_account)
            st.text_input("User", value=st.session_state.snowflake_user, 
                         disabled=DEBUG_MODE, key="user_input", on_change=update_user)
            st.text_input("Personal Access Token", value=st.session_state.snowflake_token, 
                         type="password", disabled=DEBUG_MODE, key="token_input", on_change=update_token)
            st.text_input("Warehouse", value=st.session_state.snowflake_warehouse, 
                         disabled=DEBUG_MODE, key="warehouse_input", on_change=update_warehouse)
            st.text_input("Database", value=st.session_state.snowflake_database, 
                         disabled=DEBUG_MODE, key="database_input", on_change=update_database)
            st.text_input("Schema", value=st.session_state.snowflake_schema, 
                         disabled=DEBUG_MODE, key="schema_input", on_change=update_schema)
            
        st.sidebar.title("2. Connect to Snowflake")
        with st.sidebar.expander("Connect", expanded=False):
            connect_button = st.button("Connect to Snowflake")
        if connect_button:
            # Ensure session state is updated with the latest input values
            if "account_input" in st.session_state:
                st.session_state.snowflake_account = st.session_state.account_input
            if "user_input" in st.session_state:
                st.session_state.snowflake_user = st.session_state.user_input
            if "token_input" in st.session_state:
                st.session_state.snowflake_token = st.session_state.token_input
            if "warehouse_input" in st.session_state:
                st.session_state.snowflake_warehouse = st.session_state.warehouse_input
            if "database_input" in st.session_state:
                st.session_state.snowflake_database = st.session_state.database_input
            if "schema_input" in st.session_state:
                st.session_state.snowflake_schema = st.session_state.schema_input
                
            #print(f"Account: {st.session_state.snowflake_account}")
            #print(f"User: {st.session_state.snowflake_user}")
            #print(f"Token: {st.session_state.snowflake_token}")
            #print(f"Warehouse: {st.session_state.snowflake_warehouse}")
            #print(f"Database: {st.session_state.snowflake_database}")
            #print(f"Schema: {st.session_state.snowflake_schema}")
            
            if not st.session_state.snowflake_token:
                st.error("Please provide a valid Personal Access Token.")
            elif not st.session_state.snowflake_account:
                st.error("Please provide a valid Account.")
            elif not st.session_state.snowflake_user:
                st.error("Please provide a valid User.")
            else:
                try:
                    conn = snowflake.connector.connect(
                        account=st.session_state.snowflake_account,
                        user=st.session_state.snowflake_user,
                        authenticator='snowflake',
                        password=st.session_state.snowflake_token,
                        warehouse=st.session_state.snowflake_warehouse,
                        database=st.session_state.snowflake_database,
                        schema=st.session_state.snowflake_schema
                    )
                    st.session_state.CONN = conn
                    st.success(f"Connection successful to {st.session_state.snowflake_account}!")
                    connection_parameters = {
                        "account": st.session_state.snowflake_account,
                        "user": st.session_state.snowflake_user,
                        "authenticator": 'snowflake',
                        "password": st.session_state.snowflake_token,
                        "warehouse": st.session_state.snowflake_warehouse,
                        "database": st.session_state.snowflake_database,
                        "schema": st.session_state.snowflake_schema,
                    }
                    # Initialize the global session variable
                    global session
                    session = Session.builder.configs(connection_parameters).create()
                    st.session_state.session = session  # Also store in session state for persistence
                    #cur = conn.cursor()
                    #cur.execute("SELECT CURRENT_VERSION(), CURRENT_WAREHOUSE(), CURRENT_DATABASE(), CURRENT_SCHEMA()")
                    #result = cur.fetchall()
                    #st.write("Connection Info:")
                    #for row in result:
                        #st.write(row)
                    #cur.close()
                except DatabaseError as e:
                    st.error(f"Failed to connect: {e}")
                    _, btn_container, _ = st.columns([2, 6, 2])
        st.sidebar.title("3. Semantic Model")
        with st.sidebar.expander("Select Semantic Model", expanded=False):
            #st.markdown("Select the semantic model to use for the Analyst API.")
            #st.markdown("You can upload your own semantic model YAML files to the specified stage in Snowflake and add the path here.")
            #st.markdown("Make sure the semantic model is compatible with the Analyst API.")
            st.selectbox("Select Model", AVAILABLE_SEMANTIC_MODELS_PATHS,
                format_func=lambda s: s.split("/")[-1],
                key="selected_semantic_model_path",
                on_change=reset_session_state,
                label_visibility="collapsed"
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
    print(f"User input: {user_input}")
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
    print(f"New user message: {new_user_message}")
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
    print("Inside get analyst response")
    
    # Get values from input boxes or session state
    account = st.session_state.account_input if "account_input" in st.session_state else st.session_state.snowflake_account
    user = st.session_state.user_input if "user_input" in st.session_state else st.session_state.snowflake_user
    token = st.session_state.token_input if "token_input" in st.session_state else st.session_state.snowflake_token
    warehouse = st.session_state.warehouse_input if "warehouse_input" in st.session_state else st.session_state.snowflake_warehouse
    database = st.session_state.database_input if "database_input" in st.session_state else st.session_state.snowflake_database
    schema = st.session_state.schema_input if "schema_input" in st.session_state else st.session_state.snowflake_schema
    
    # For minimal debugging
    print(f"Schema: {schema}")
    
    # Set up API endpoint
    HOST = account + ".snowflakecomputing.com"
    api_url = f"https://{HOST}{API_ENDPOINT}"
    
    # Prepare the request body
    request_body = {
        "messages": messages,
        "semantic_model_file": f"@{st.session_state.selected_semantic_model_path}",
    }
    
    # Debug session state
    print("\n==== Session state debug ====")
    print(f"CONN in session state: {'CONN' in st.session_state}")
    if 'CONN' in st.session_state:
        print(f"CONN is None: {st.session_state.CONN is None}")
        if st.session_state.CONN is not None:
            print(f"Has rest attribute: {hasattr(st.session_state.CONN, 'rest')}")
            if hasattr(st.session_state.CONN, 'rest'):
                print(f"rest is None: {st.session_state.CONN.rest is None}")
                if st.session_state.CONN.rest is not None:
                    print(f"Has token attribute: {hasattr(st.session_state.CONN.rest, 'token')}")
                    if hasattr(st.session_state.CONN.rest, 'token'):
                        print(f"Token is None: {st.session_state.CONN.rest.token is None}")
                        print(f"Token length: {len(st.session_state.CONN.rest.token) if st.session_state.CONN.rest.token else 0}")
    print("==== End session state debug ====")

    try:
        print("\n==== Making API request ====")
        
        # Determine which token to use for the API request
        token_to_use = None
        
        # First try to get from session state CONN object
        if ("CONN" in st.session_state and 
            st.session_state.CONN is not None and 
            hasattr(st.session_state.CONN, "rest") and 
            st.session_state.CONN.rest is not None and 
            hasattr(st.session_state.CONN.rest, "token") and 
            st.session_state.CONN.rest.token is not None):
            token_to_use = st.session_state.CONN.rest.token
            print("Using token from session state (CONN)")
        
        # If not in CONN, use the token we got earlier
        if token_to_use is None:
            token_to_use = token
            print("Using token from function parameters")
        
        # Prepare headers with token
        headers = {
            "Authorization": f'Snowflake Token="{token_to_use}"',
            "Content-Type": "application/json",
        }
        
        # Send a POST request to the Cortex Analyst API endpoint
        resp = requests.post(
            url=api_url,
            json=request_body,
            headers=headers,
            timeout=API_TIMEOUT/1000,  # Convert milliseconds to seconds
        )
        
        print(f"\n==== Response received ====")
        print(f"Status code: {resp.status_code}")
        request_id = resp.headers.get("X-Snowflake-Request-Id", "unknown")
        print(f"Request ID: {request_id}")
        print(f"Response headers: {resp.headers}")
        
        if resp.status_code < 400:
            print("Success response (status < 400)")
            response_json = resp.json()
            print(f"Response JSON: {response_json}")
            
            # Validate response structure
            if "message" in response_json and "content" in response_json["message"]:
                print("Valid response structure found")
                return {**response_json, "request_id": request_id}, None
            else:
                print("Invalid response structure")
                error_msg = "Received an invalid response format from the Analyst API."
                return {"request_id": request_id}, error_msg
        else:
            print(f"Error response: {resp.status_code}")
            # Craft readable error message for HTTP errors
            try:
                parsed_content = resp.json()
                print(f"Error content: {parsed_content}")
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
            except Exception as json_err:
                print(f"Failed to parse error response as JSON: {json_err}")
                print(f"Raw response content: {resp.content}")
                error_msg = f"Failed request (id: {request_id}) with status {resp.status_code}: {resp.content}"
            
            return {"request_id": request_id}, error_msg
            
    except requests.exceptions.Timeout:
        print(f"\n==== Timeout Exception ====")
        print(f"Request timed out after {API_TIMEOUT/1000} seconds")
        error_msg = f"Request timed out after {API_TIMEOUT/1000} seconds. Please try again."
        return {"request_id": "timeout"}, error_msg
    except requests.exceptions.RequestException as e:
        print(f"\n==== Request Exception ====")
        print(f"Type: {type(e).__name__}")
        print(f"Message: {str(e)}")
        print(f"Details: {repr(e)}")
        error_msg = f"Request error: {str(e)}"
        return {"request_id": "error"}, error_msg
    except Exception as e:
        print(f"\n==== Unexpected Exception ====")
        print(f"Type: {type(e).__name__}")
        print(f"Message: {str(e)}")
        print(f"Details: {repr(e)}")
        import traceback
        traceback.print_exc()
        error_msg = f"Unexpected error: {str(e)}"
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
        # First try to execute with default settings
        try:
            df = current_session.sql(query).to_pandas()
        except Exception as e:
            if "Cannot convert non-finite values (NA or inf) to integer" in str(e):
                # Try again with pandas options to handle NA values
                # Create a temporary table with the results
                temp_table_name = f"TEMP_QUERY_RESULT_{int(time.time())}"
                current_session.sql(f"CREATE TEMPORARY TABLE {temp_table_name} AS {query}").collect()
                
                # Fetch the schema to determine column types
                schema_query = f"DESCRIBE TABLE {temp_table_name}"
                schema_df = current_session.sql(schema_query).to_pandas()
                
                # Construct a query that casts problematic columns to appropriate types
                columns = []
                for _, row in schema_df.iterrows():
                    col_name = row['name']
                    col_type = row['type']
                    
                    # For numeric columns that might have NULLs, use COALESCE or explicit casting
                    if 'INT' in col_type.upper():
                        columns.append(f"COALESCE({col_name}, 0) AS {col_name}")
                    elif 'FLOAT' in col_type.upper() or 'DOUBLE' in col_type.upper() or 'DECIMAL' in col_type.upper() or 'NUMERIC' in col_type.upper():
                        columns.append(f"COALESCE({col_name}, 0.0) AS {col_name}")
                    else:
                        columns.append(col_name)
                
                # Execute the modified query
                safe_query = f"SELECT {', '.join(columns)} FROM {temp_table_name}"
                df = current_session.sql(safe_query).to_pandas()
                
                # Clean up the temporary table
                current_session.sql(f"DROP TABLE IF EXISTS {temp_table_name}").collect()
            else:
                # If it's a different error, re-raise it
                raise
        
        return df, None
    except Exception as e:
        return None, f"Error executing query: {str(e)}"


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
