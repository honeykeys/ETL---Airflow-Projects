import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
import requests

# This is a very simple Airflow project that showcases the fundamentals in creating a DAG
# We ping the Met public API for collection data and then store that data into a CSV
# We schedule a job for each Sunday, starting on 4/24/2024 to update the csv file
# with a new batch of data


OUTPUT_FILE = "met_object_data.csv"

def fetch_and_store_data():
    api_url = "https://collectionapi.metmuseum.org/public/collection/v1/objects" 
    response = requests.get(api_url)

    if response.status_code == 200:
        json_data = response.json()
        total_objects = json_data.get('total', 0)
        object_ids = json_data.get('objectIDs', [])

        with open(OUTPUT_FILE, 'a') as f:
            f.write(f"{datetime.now()}, {total_objects}, {object_ids}\n")
    else:
        raise ValueError(f"API Error: Status Code {response.status_code}")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 4, 24), 
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'met_data_collection',
    default_args=default_args,
    schedule_interval='0 0 * * 0'
)

collect_data_task = PythonOperator(
    task_id='collect_met_data',
    python_callable=fetch_and_store_data,
    dag=dag
)