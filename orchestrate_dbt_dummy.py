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

ops = {}
for node in manifest['nodes'].keys():
    if node.split('.')[0] == 'model':
        task = DummyOperator(
            task_id = node,
            dag=dag,
        )
        ops[node] = task

sources = set([s.split('.')[2] for s in manifest['sources'].keys()])
srcs = {}
for src in sources:
    task = DummyOperator(
        task_id = src,
        dag=dag,
    )
    srcs[src] = task

for parent in manifest['child_map'].keys():
    if parent.split('.')[0] == 'model':
        for child in manifest['child_map'][parent]:
            if child.split('.')[0] == 'model':
                ops[parent] >> ops[child]

for node in manifest['parent_map'].keys():
    if node.split('.')[0] == 'model':
        for parent in manifest['parent_map'][node]:
            parent_split = parent.split('.')
            if parent_split[0] == 'source':
                src_sys = parent_split[2]
                srcs[src_sys] >> ops[node]
