import json
from datetime import datetime, timedelta

import pendulum
import requests
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2018, 9, 12, tzinfo=pendulum.timezone('America/Los_Angeles')),
    'email': ['jharris@coh.org'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
    }

dag = DAG('tableau_server_maintenance', default_args=default_args, catchup=False, schedule_interval='0 1 * * *')


def backup():
    print("I no longer do anything.")



bs = PythonOperator(
        task_id='placeholder_task',
        python_callable=backup,
        dag=dag
        )
