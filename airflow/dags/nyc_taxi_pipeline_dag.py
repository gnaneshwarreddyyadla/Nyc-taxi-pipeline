import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

PROJECT_ROOT = "/Users/yedla786gmail.com/nyc-taxi-pipeline"
VENV_PYTHON  = f"{PROJECT_ROOT}/.venv/bin/python"
DBT_BIN      = f"{PROJECT_ROOT}/.venv/bin/dbt"
DBT_DIR      = f"{PROJECT_ROOT}/dbt/taxi_pipeline"
ENV_FILE     = f"{PROJECT_ROOT}/.env"
S3_BUCKET    = "nyc-taxi-pipeline-gani"
KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC  = "nyc_taxi_trips"

default_args = {
    "owner": "gnaneshwar",
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
    "start_date": datetime(2025, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
}

def copy_into_snowflake(**context):
    import snowflake.connector
    from dotenv import load_dotenv
    load_dotenv(ENV_FILE)

    now = datetime.utcnow()
    s3_prefix = (
        
        f"year={now.year}/month={now.month:02d}/day={now.day:02d}/"
    )

    conn = snowflake.connector.connect(
        account   = os.getenv("SNOWFLAKE_ACCOUNT", "AGPHTGX-SCC62357"),
        user      = os.getenv("SNOWFLAKE_USER", "GNANESHWAR"),
        password  = os.getenv("SNOWFLAKE_PASSWORD"),
        warehouse = "TAXI_WH",
        database  = "TAXI_DB",
        schema    = "RAW",
        role      = "ACCOUNTADMIN",
    )
    cur = conn.cursor()
    try:
        print("Truncating RAW.taxi_trips...")
        cur.execute("TRUNCATE TABLE IF EXISTS RAW.taxi_trips;")

        copy_sql = f"""
            COPY INTO RAW.taxi_trips
            FROM '{s3_prefix}'
            CREDENTIALS = (
                AWS_KEY_ID     = '{os.getenv("AWS_ACCESS_KEY_ID")}',
                AWS_SECRET_KEY = '{os.getenv("AWS_SECRET_ACCESS_KEY")}'
            )
            FILE_FORMAT = (TYPE = 'JSON' STRIP_OUTER_ARRAY = TRUE)
            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
            ON_ERROR = CONTINUE;
        """
        print(f"COPY INTO from {s3_prefix}...")
        cur.execute(copy_sql)
        results = cur.fetchall()
        print(f"Done: {results}")
    finally:
        cur.close()
        conn.close()

with DAG(
    dag_id            = "nyc_taxi_pipeline",
    default_args      = default_args,
    description       = "Kafka → S3 → Snowflake → dbt",
    schedule_interval = "@daily",
    catchup           = False,
    tags              = ["nyc-taxi", "kafka", "snowflake", "dbt"],
) as dag:

    t1_produce = BashOperator(
        task_id      = "produce_to_kafka",
        bash_command = f"cd {PROJECT_ROOT} && {VENV_PYTHON} kafka/producer.py",
    )

    t2_reset = BashOperator(
        task_id      = "reset_consumer_group",
        bash_command = (
            f"{PROJECT_ROOT}/.venv/bin/kafka-consumer-groups.sh "
            f"--bootstrap-server {KAFKA_BROKER} "
            f"--group nyc_taxi_s3_group "
            f"--topic {KAFKA_TOPIC} "
            f"--reset-offsets --to-earliest --execute 2>/dev/null || true"
        ),
    )

    t3_consume = BashOperator(
        task_id           = "consume_to_s3",
        bash_command      = f"cd {PROJECT_ROOT} && {VENV_PYTHON} kafka/consumer_s3.py",
        execution_timeout = timedelta(minutes=15),
    )

    t4_copy = PythonOperator(
        task_id         = "copy_into_snowflake",
        python_callable = copy_into_snowflake,
        provide_context = True,
    )

    t5_dbt = BashOperator(
        task_id      = "dbt_run",
        bash_command = f"cd {DBT_DIR} && {DBT_BIN} run --no-partial-parse",
    )

    t1_produce >> t2_reset >> t3_consume >> t4_copy >> t5_dbt
