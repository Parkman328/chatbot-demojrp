FROM python:3.11-slim

WORKDIR /app

# Copy requirements first for better caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application
COPY . .

# Railway specific configuration
# Railway automatically assigns a PORT environment variable
# We don't need to explicitly EXPOSE the port for Railway
ENV DEBUG=false

# Command to run the application
# Use $PORT from Railway or default to 8080 if not provided
CMD ["sh", "-c", "streamlit run streamlit_app.py --server.port=${PORT:-8080} --server.address=0.0.0.0"]
