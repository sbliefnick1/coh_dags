from datetime import datetime

import pendulum
from airflow import DAG
from airflow.operators.dummy import DummyOperator
import requests


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2018, 9, 12, tzinfo=pendulum.timezone('America/Los_Angeles')),
    'concurrency': 8,
    'email': ['jharris@coh.org'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    }

dag = DAG('orchestrate_dbt_dummy', default_args=default_args, catchup=False, schedule_interval='0 1 * * *')

url = 'http://build.coh.org:37000/manifest.json'
manifest = requests.get(url).json()

for node in manifest['nodes'].keys():
    task = DummyOperator(
        task_id = node,
        dag=dag,
    )

parents = manifest['child_map'].keys()
for node in parents:
    if node.split('.')[0] == 'model':
        for child in manifest['child_map'][node]:
            print(f'{node} >> {child}')
