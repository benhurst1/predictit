from datetime import datetime, timedelta, date
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
import logging
import boto3
import requests
import json

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

dag = DAG(
    "pull_predictit_data",
    default_args=default_args,
    description="Pull predictit data and store in s3 bucket",
    schedule_interval="0 0 * * *",
    start_date=datetime(2024, 1, 3),
    catchup=False,
)


def request_json():
    logging.info("Started task")

    aws_access = Variable.get("AWS_ACCESS_KEY")
    aws_secret_access = Variable.get("AWS_SECRET_ACCESS_KEY")
    bucket = Variable.get("BUCKET")

    filename = f"predictit-data-{date.today()}"
    s3 = boto3.client(
        "s3", aws_access_key_id=aws_access, aws_secret_access_key=aws_secret_access
    )

    logging.info("Connecting")
    try:
        response = requests.get("https://www.predictit.org/api/marketdata/all/")
        logging.info("Connected")
    except:
        logging.info("Connection failed")

    data = response.json()
    s3.put_object(Body=str(json.dumps(data)), Bucket=bucket, Key=f"data/{filename}")


pull_data_task = PythonOperator(
    task_id="pull_data_task",
    python_callable=request_json,
    dag=dag,
)

pull_data_task
