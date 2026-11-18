# Use an official lightweight Python image
FROM python:3.14-slim

# Set working directory inside the container
WORKDIR /app

# Install required Python packages (e.g., pandas, google-cloud-bigquery)
RUN pip install --no-cache-dir -r requirements.txt

# Copy your local ETL script into the container
COPY app/main.py

# Command to run when the container starts
CMD ["python", "main.py"]