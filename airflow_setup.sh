#!/bin/bash
set -e
PROJECT_ROOT="/Users/yedla786gmail.com/nyc-taxi-pipeline"
AIRFLOW_HOME="$PROJECT_ROOT/airflow"
VENV="$PROJECT_ROOT/.venv"

source "$VENV/bin/activate"

echo "Installing Airflow 2.9.3..."
AIRFLOW_VERSION=2.9.3
PYTHON_VERSION="$(python --version | cut -d ' ' -f 2 | cut -d '.' -f 1-2)"
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}" --quiet

echo "Installing providers..."
pip install snowflake-connector-python apache-airflow-providers-snowflake python-dotenv --quiet

export AIRFLOW_HOME="$AIRFLOW_HOME"
mkdir -p "$AIRFLOW_HOME/dags"
mkdir -p "$AIRFLOW_HOME/logs"

echo "export AIRFLOW_HOME=$AIRFLOW_HOME" >> ~/.zshrc

echo "Initializing DB..."
airflow db migrate

echo "Creating admin user..."
airflow users create \
    --username admin \
    --password admin123 \
    --firstname Gnaneshwar \
    --lastname Yadla \
    --role Admin \
    --email gnaneshwarreddy056@gmail.com 2>/dev/null || echo "User already exists."

echo ""
echo "SETUP COMPLETE!"
