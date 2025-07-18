#!/bin/bash
set -e

# Step 1: Install dependencies
if [ -e "/opt/airflow/requirements.txt" ]; then
  echo "Installing Python dependencies from requirements.txt..."
  pip install --upgrade pip
  pip install -r /opt/airflow/requirements.txt
else
  echo "No requirements.txt found, skipping Python dependency install."
fi

# Step 2: Initialize Airflow DB if not already
if [ ! -f "/opt/airflow/airflow.db" ]; then
  echo "Initializing Airflow database..."
  airflow db init

  echo "Creating default admin user..."
  airflow users create \
    --username admin \
    --firstname Nilkamal \
    --lastname Mahato \
    --role Admin \
    --email nilkamal.35@gmail.com \
    --password admin@123

  echo "  - AIRFLOW_WEBSERVER_USER_USERNAME=admin"
  echo "  - AIRFLOW_WEBSERVER_USER_FIRSTNAME=Nilkamal"
  echo "  - AIRFLOW_WEBSERVER_USER_LASTNAME=Mahato"
  echo "  - AIRFLOW_WEBSERVER_USER_EMAIL=nilkamal.35@gmail.com"
  echo "  - AIRFLOW_WEBSERVER_USER_PASSWORD=admin@123"
  echo "  - AIRFLOW_WEBSERVER_USER_ROLE=Admin"
else
  echo "Airflow database already exists. Skipping DB init."
fi

# Step 3: Start the webserver
echo "Starting Airflow webserver..."
exec airflow webserver
