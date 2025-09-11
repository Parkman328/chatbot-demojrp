import json
import time
from typing import Dict, List, Optional, Tuple

import streamlit as st
import pandas as pd
import snowflake.connector
from snowflake.connector.errors import DatabaseError
from snowflake.snowpark import Session
from snowflake.snowpark.exceptions import SnowparkSQLException

secrets = st.secrets["connections"]["snowflake"]

# Inputs for connection details (can also use st.secrets)
account=secrets["ACCOUNT"]
user=secrets["USER"]
authenticator='snowflake'
token=secrets["PAT"]
warehouse=secrets["WAREHOUSE"]
database=secrets["DATABASE"]
schema=secrets["SCHEMA"]

# List of available semantic model paths in the format: <DATABASE>.<SCHEMA>.<STAGE>/<FILE-NAME>
# Each path points to a YAML file defining a semantic model

AVAILABLE_SEMANTIC_MODELS_PATHS = [
    "CORTEX_DEMOS.CONTOSO.ANALYST_STAGE/ContosoDemo.yaml"
]
API_ENDPOINT = "/api/v2/cortex/analyst/message"
FEEDBACK_API_ENDPOINT = "/api/v2/cortex/analyst/feedback"
API_TIMEOUT = 50000  # in milliseconds


def main():
    # Initialize session state
    if "messages" not in st.session_state:
        reset_session_state()
    show_header_and_sidebar()
    if len(st.session_state.messages) == 0:
        process_user_input("What questions can I ask?")
    display_conversation()
    #handle_user_inputs()
    #handle_error_notifications()
    display_connectbutton()


def reset_session_state():
    """Reset important session state elements."""
    st.session_state.messages = []  # List to store conversation messages
    st.session_state.active_suggestion = None  # Currently selected suggestion


def show_header_and_sidebar():
    """Display the header and sidebar of the app."""
# Show title and description.
    st.title("Snowflake Cortext Analyst")
    st.write(
        "A simple chat app using Cortext Analyst and the OpenAI API. "
    )

    # Sidebar with a reset button
    with st.sidebar:
        st.selectbox(   
            "Selected semantic model:",
            AVAILABLE_SEMANTIC_MODELS_PATHS,
            format_func=lambda s: s.split("/")[-1],
            key="selected_semantic_model_path",
            on_change=reset_session_state,
        )
        st.divider()
        st.markdown("## Snowflake Connection Details")
        st.text_input("Account", value=account, disabled=True)
        st.text_input("User", value=user, disabled=True)
        st.text_input("Personal Access Token", value=token, type="password", disabled=True)
        st.text_input("Warehouse", value=warehouse, disabled=True)
        st.text_input("Database", value=database, disabled=True)
        st.text_input("Schema", value=schema, disabled=True)
        
    
        connect_button = st.button("Connect to Snowflake")
        if connect_button:
            if not token:
                st.error("Please provide a valid Personal Access Token.")
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
                    session = Session.builder.configs(connection_parameters).create()
                    cur = conn.cursor()
                    cur.execute("SELECT CURRENT_VERSION(), CURRENT_WAREHOUSE(), CURRENT_DATABASE(), CURRENT_SCHEMA()")
                    result = cur.fetchall()
                    st.write("Connection Info:")
                    for row in result:
                        st.write(row)
                    cur.close()
                    conn.close()
                except DatabaseError as e:
                    st.error(f"Failed to connect: {e}")
                    _, btn_container, _ = st.columns([2, 6, 2])
                    if btn_container.button("Clear Chat History", use_container_width=True):
                        reset_session_state()

def display_connectbutton():
    st.write("Click the button on the left sidebar to connect to Snowflake.")
    st.markdown("Make sure to enter your connection details correctly.")


def handle_user_inputs():
    """Handle user inputs from the chat interface."""
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
    if st.session_state.get("fire_API_error_notify"):
        st.toast("An API error has occured!", icon="🚨")
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
            response, error_msg = get_analyst_response(st.session_state.messages)
            if error_msg is None:
                analyst_message = {
                    "role": "analyst",
                    "content": response["message"]["content"],
                    "request_id": response["request_id"],
                }
            else:
                analyst_message = {
                    "role": "analyst",
                    "content": [{"type": "text", "text": error_msg}],
                    "request_id": response["request_id"],
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
        Optional[Dict]: The response from the Cortex Analyst API.
    """
    # Prepare the request body with the user's prompt
    request_body = {
        "messages": messages,
        "semantic_model_file": f"@{st.session_state.selected_semantic_model_path}",
    }

    # Send a POST request to the Cortex Analyst API endpoint
    # Adjusted to use positional arguments as per the API's requirement
    resp = _snowflake.send_snow_api_request(
        "POST",  # method
        API_ENDPOINT,  # path
        {},  # headers
        {},  # params
        request_body,  # body
        None,  # request_guid
        API_TIMEOUT,  # timeout in milliseconds
    )

    # Content is a string with serialized JSON object
    parsed_content = json.loads(resp["content"])

    # Check if the response is successful
    if resp["status"] < 400:
        # Return the content of the response as a JSON object
        return parsed_content, None
    else:
        # Craft readable error message
        error_msg = f"""
🚨 An Analyst API error has occurred 🚨

* response code: `{resp['status']}`
* request-id: `{parsed_content['request_id']}`
* error code: `{parsed_content['error_code']}`

Message:
```
{parsed_content['message']}
```
        """
        return parsed_content, error_msg


def display_conversation():
    """
    Display the conversation history between the user and the assistant.
    """
    for idx, message in enumerate(st.session_state.messages):
        role = message["role"]
        content = message["content"]
        with st.chat_message(role):
            display_message(content, idx)


def display_message(content: List[Dict[str, str]], message_index: int):
    """
    Display a single message content.

    Args:
        content (List[Dict[str, str]]): The message content.
        message_index (int): The index of the message.
    """
    for item in content:
        if item["type"] == "text":
            st.markdown(item["text"])
        elif item["type"] == "suggestions":
            # Display suggestions as buttons
            for suggestion_index, suggestion in enumerate(item["suggestions"]):
                if st.button(
                    suggestion, key=f"suggestion_{message_index}_{suggestion_index}"
                ):
                    st.session_state.active_suggestion = suggestion
        elif item["type"] == "sql":
            # Display the SQL query and results
            display_sql_query(item["statement"], message_index)
        else:
            # Handle other content types if necessary
            pass


@st.cache_data(show_spinner=False)
def get_query_exec_result(query: str) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
    """
    Execute the SQL query and convert the results to a pandas DataFrame.

    Args:
        query (str): The SQL query.

    Returns:
        Tuple[Optional[pd.DataFrame], Optional[str]]: The query results and the error message.
    """
    global session
    try:
        df = session.sql(query).to_pandas()
        return df, None
    except SnowparkSQLException as e:
        return None, str(e)


def display_sql_query(sql: str, message_index: int):
    """
    Executes the SQL query and displays the results in form of data frame and charts.

    Args:
        sql (str): The SQL query.
        message_index (int): The index of the message.
    """

    # Display the SQL query
    with st.expander("SQL Query", expanded=False):
        st.code(sql, language="sql")

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
            sql_new = f'''SELECT SNOWFLAKE.CORTEX.COMPLETE('mistral-large2','Summarize results, Show trends & Itemize top insights & trends from the following json data in less than 150 words. Data: '  ||
                                        (
                                            SELECT array_agg( object_construct(*))::string as Output from
                                                    ( {sql.replace(";", "")}  )
                                        )
                                        ) as Insights'''
            
# <---Remove this block to disable the additional Summarization feature OR use a better model than mistral-large2 to get more accurate results
            
            if len(df) <= 20:
                DataSummary = get_query_exec_result(sql_new)
                st.markdown(str(DataSummary[0].iat[0, 0]))
                
            # <---
            
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
    st.markdown("Developed by [John Park, Qlik Analytics Presales Architect](https://www.linkedin.com/in/jpark328/) | [Email](mailto:john.park@qlik.com) | [GitHub Repo](https://github.com/john-park-4519a41b/)")
    st.markdown(
    "<hr style='margin-top:2em; margin-bottom:1em;'>"
    "<div style='text-align:center; color:gray;'>" \
    "Developed for Demo Purpose Not Production Use." \
    "<br>" \
    "© 2025 Qlik, Inc. All rights reserved."
    "</div>",
    unsafe_allow_html=True
)


#reset_session_state()
# Ask user for their OpenAI API key via `st.text_input`.
# Alternatively, you can store the API key in `./.streamlit/secrets.toml` and access it
# via `st.secrets`, see https://docs.streamlit.io/develop/concepts/connections/secrets-management
#openai_api_key = st.text_input("OpenAI API Key", type="password")
#if not openai_api_key:
#    st.info("Please add your OpenAI API key to continue.", icon="🗝️")
#else:

    # Create an OpenAI client.
 #   client = OpenAI(api_key=openai_api_key)

    # Create a session state variable to store the chat messages. This ensures that the
    # messages persist across reruns.
  #  if "messages" not in st.session_state:
  #      st.session_state.messages = []

    # Display the existing chat messages via `st.chat_message`.
   # for message in st.session_state.messages:
    #    with st.chat_message(message["role"]):
     #       st.markdown(message["content"])

    # Create a chat input field to allow the user to enter a message. This will display
    # automatically at the bottom of the page.
    #if prompt := st.chat_input("What is up?"):

        # Store and display the current prompt.
     #   st.session_state.messages.append({"role": "user", "content": prompt})
      #  with st.chat_message("user"):
     #       st.markdown(prompt)

        # Generate a response using the OpenAI API.
       # stream = client.chat.completions.create(
        #    model="gpt-3.5-turbo",
           # messages=[
            #    {"role": m["role"], "content": m["content"]}
            #    for m in st.session_state.messages
            #],
            #stream=True,
        #)

        # Stream the response to the chat using `st.write_stream`, then store it in 
        # session state.
        #with st.chat_message("assistant"):
        #    response = st.write_stream(stream)
        #st.session_state.messages.append({"role": "assistant", "content": response})