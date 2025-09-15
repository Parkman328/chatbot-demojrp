# 💬 Snowflake Cortext Analyst for Qlik Cloud

A Streamlit application that integrates with Snowflake Cortext Analyst API to provide natural language querying capabilities for Qlik Cloud data.

## How to run it on your own machine

1. Install the requirements

   ```
   $ pip install -r requirements.txt
   ```

2. Run the app

   ```
   $ streamlit run streamlit_app.py
   ```

## Environment Variables

The application can be configured using the following environment variables:

```
DEBUG=true|false                # Set to true for development mode, false for production
SNOWFLAKE_ACCOUNT=<account>     # Your Snowflake account identifier
SNOWFLAKE_USER=<user>           # Your Snowflake username
SNOWFLAKE_PAT=<token>           # Your Snowflake personal access token
SNOWFLAKE_WAREHOUSE=<warehouse> # Your Snowflake warehouse
SNOWFLAKE_DATABASE=<database>   # Your Snowflake database
SNOWFLAKE_SCHEMA=<schema>       # Your Snowflake schema
SNOWFLAKE_AUTHENTICATOR=snowflake # Authentication method (default: snowflake)
```

## Docker Deployment

1. Build the Docker image

   ```
   $ docker build -t snowflake-cortext-analyst .
   ```

2. Run the container

   ```
   $ docker run -p 8501:8501 \
     -e SNOWFLAKE_ACCOUNT=<account> \
     -e SNOWFLAKE_USER=<user> \
     -e SNOWFLAKE_PAT=<token> \
     -e SNOWFLAKE_WAREHOUSE=<warehouse> \
     -e SNOWFLAKE_DATABASE=<database> \
     -e SNOWFLAKE_SCHEMA=<schema> \
     snowflake-cortext-analyst
   ```

## Deploying to Snowpark Container Service

1. Build and tag your Docker image

   ```
   $ docker build -t <your-registry>/snowflake-cortext-analyst:latest .
   $ docker push <your-registry>/snowflake-cortext-analyst:latest
   ```

2. Create a specification file `spec.json` for your Snowpark Container Service:

   ```json
   {
     "name": "snowflake-cortext-analyst",
     "image": "<your-registry>/snowflake-cortext-analyst:latest",
     "env": {
       "SNOWFLAKE_ACCOUNT": "${SNOWFLAKE_ACCOUNT}",
       "SNOWFLAKE_USER": "${SNOWFLAKE_USER}",
       "SNOWFLAKE_PAT": "${SNOWFLAKE_PAT}",
       "SNOWFLAKE_WAREHOUSE": "${SNOWFLAKE_WAREHOUSE}",
       "SNOWFLAKE_DATABASE": "${SNOWFLAKE_DATABASE}",
       "SNOWFLAKE_SCHEMA": "${SNOWFLAKE_SCHEMA}"
     },
     "ports": [8501],
     "resources": {
       "cpu": "1",
       "memory": "2Gi"
     }
   }
   ```

3. Deploy to Snowpark Container Service using Snowflake SQL:

   ```sql
   -- Create a service
   CREATE SERVICE snowflake_cortext_analyst
     IN COMPUTE POOL your_compute_pool
     FROM SPECIFICATION '/path/to/spec.json'
     MIN_INSTANCES = 1
     MAX_INSTANCES = 1;
   
   -- Get the service URL
   DESCRIBE SERVICE snowflake_cortext_analyst;
   ```

4. Access your application at the provided endpoint URL
