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

with DAG('orchestrate_metrics_qa', default_args=default_args, catchup=False, schedule_interval='0 20 * * *') as dag:

    url = 'https://vpxrstudio.coh.org/content/5fceaff8-8811-41ac-be8b-88aae904b2b6/nodes/'
    data = requests.get(url, verify=False).json()

    def prep_name(node_name):
        return node_name.lower().replace(' ', '_').replace('(', '').replace(')', '').replace('/', '')

    n = {}
    for node in data:
        node = f"{prep_name(node['type'])}_{prep_name(node['name'])}"
        task = DummyOperator(task_id=node)
        n[node] = task

    for node in data:
        for parent in node['parents']:
            c = f"{prep_name(node['type'])}_{prep_name(node['name'])}"
            p = f"{prep_name(parent['type'])}_{prep_name(parent['name'])}"
            n.get(p) >> n.get(c)
