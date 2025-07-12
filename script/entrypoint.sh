#!/bin/bash
set -e

if [ -e "/opt/airflow/requirements.txt" ]; then
  echo "Installing Python dependencies from requirements.txt..."
  python -m pip install --upgrade pip
  python -m pip install -r /opt/airflow/requirements.txt
  echo "Python dependencies installed."
else
  echo "requirements.txt not found, skipping dependency installation."
fi

if [ ! -f "/opt/airflow/airflow.db" ]; then
  echo "Initializing Airflow database..."
  airflow db migrate
  echo "Airflow database migration complete."

  echo "Attempting to create initial admin user via environment variables..."
  AIRFLOW_WEBSERVER_USER_USERNAME=admin
  AIRFLOW_WEBSERVER_USER_FIRSTNAME=Admin
  AIRFLOW_WEBSERVER_USER_LASTNAME=User
  AIRFLOW_WEBSERVER_USER_EMAIL=nilkamal.35@gmail.com
  AIRFLOW_WEBSERVER_USER_PASSWORD=admin@123
  AIRFLOW_WEBSERVER_USER_ROLE=Admin
  echo "Initial admin user creation typically relies on webserver environment variables in Airflow 3.0.0."
  echo "Please ensure these are set in your docker-compose.yml for the webserver service:"
  echo "  - AIRFLOW_WEBSERVER_USER_USERNAME=admin"
  echo "  - AIRFLOW_WEBSERVER_USER_FIRSTNAME=Admin"
  echo "  - AIRFLOW_WEBSERVER_USER_LASTNAME=User"
  echo "  - AIRFLOW_WEBSERVER_USER_EMAIL=nilkamal.35@gmail.com"
  echo "  - AIRFLOW_WEBSERVER_USER_PASSWORD=admin@123"
  echo "  - AIRFLOW_WEBSERVER_USER_ROLE=Admin"
else
  echo "Airflow database already exists, skipping initial migration and user creation."
  airflow db migrate
  echo "Airflow database migration (for updates) complete."
fi


echo "Starting Airflow webserver..."
exec airflow api-server