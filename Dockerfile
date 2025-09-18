FROM python:3.11-slim

WORKDIR /app

# Copy requirements first for better caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Create .streamlit directory and add configuration
RUN mkdir -p .streamlit
RUN echo '[server]\nheadless = true\nenableCORS = false\nenableXsrfProtection = false\nport = 8080\nbaseUrlPath = ""' > .streamlit/config.toml

# Copy the rest of the application
COPY . .

# Railway specific configuration
# Railway automatically assigns a PORT environment variable
ENV DEBUG=false

# Command to run the application
# Use $PORT from Railway or default to 8080 if not provided
CMD ["sh", "-c", "streamlit run streamlit_app.py --server.port=8080 --server.address=0.0.0.0"]
