import streamlit as st
import snowflake.connector
from snowflake.connector.errors import DatabaseError

secrets = st.secrets["connections"]["snowflake"]

# --- Streamlit UI ---
st.title("Snowflake Connection using PAT")

# Inputs for connection details (can also use st.secrets)
account=secrets["ACCOUNT"]
user=secrets["USER"]
authenticator='snowflake'
token=secrets["PAT"]
warehouse=secrets["WAREHOUSE"]
database=secrets["DATABASE"]
schema=secrets["SCHEMA"]

# Show title and description.
st.title("Snowflake Cortext Analyst")
st.write(
    "A simple chat app using Cortext Analyst and the OpenAI API. "
)


if connect_button:
    if not token:
        st.error("Please provide a valid Personal Access Token.")
    else:
        try:
            # --- Snowflake connection using OAuth token ---
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

            # Example query
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
            st.markdown(prompt)

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
