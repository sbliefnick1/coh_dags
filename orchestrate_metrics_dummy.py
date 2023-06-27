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

with DAG('orchestrate_metrics_dummy', default_args=default_args, catchup=False, schedule_interval='0 1 * * *') as dag:

    url = 'https://vpxrstudio.coh.org/content/5fceaff8-8811-41ac-be8b-88aae904b2b6/nodes/'
    data = requests.get(url, verify=False).json()

    def prep_name(node_name):
        return node_name.lower().replace(' ', '_').replace('(', '').replace(')', '').replace('/', '')

    nodes = list(set([f"{prep_name(n['type'])}_{prep_name(n['name'])}" for n in data]))

    n = {}
    for node in nodes:
        task = DummyOperator(task_id=node)
        n[node] = task

    for node in nodes:
        if len(node['parents']) > 0:
            for parent in node['parents']:
                c = f"{node['type']}_{node['name']}"
                p = f"{parent['type']}_{parent['name']}"
                n.get(p) >> n.get(c)
