from datetime import datetime

import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
import requests
import json
import time


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2018, 9, 12, tzinfo=pendulum.timezone('America/Los_Angeles')),
    'email': ['jharris@coh.org', 'nbyers@coh.org'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
}

with DAG('orchestrate_metrics_prod', default_args=default_args, catchup=False, schedule_interval='0 20 * * *') as dag:

    base_url = 'https://vpxrstudio.coh.org/content/5fceaff8-8811-41ac-be8b-88aae904b2b6'
    json_path = '/var/nfsshare/etl_deps/metrics_deps.json'
    with open(json_path, 'r') as infile:
        data = json.load(infile)
    token = Variable.get('metrics_api_token')

    def prep_name(node_name):
        return node_name.lower().replace(' ', '_').replace('(', '').replace(')', '').replace('/', '')
    
    def refresh_node(node_url, api_token):
        resp = requests.put(
            node_url,
            headers={'x-access-token': api_token},
            verify=False,
        )
        if resp.status_code != 200:
            print(f'Sending PUT request to {node_url}')
            print(resp.content)
            raise ValueError(f'PUT operation failed with response')
        time.sleep(10)

    n = {}
    for node in data:
        node_type = prep_name(node['type'])
        refresh_url = f"{base_url}/refresh/{node_type}/prod/{node['id']}"
        node = f"{node_type}_{prep_name(node['name'])}"

        if node_type == 'base':
            weight = 3
        elif node_type == 'instance':
            weight = 2
        else:
            weight = 1

        task = PythonOperator(
            task_id=node,
            python_callable=refresh_node,
            op_kwargs={'node_url': refresh_url, 'api_token': Variable.get('metrics_api_token')},
            pool='metrics_pool',
            priority_weight=weight,
        )
        n[node] = task

    for node in data:
        for parent in node['parents']:
            c = f"{prep_name(node['type'])}_{prep_name(node['name'])}"
            p = f"{prep_name(parent['type'])}_{prep_name(parent['name'])}"
            n.get(p) >> n.get(c)
