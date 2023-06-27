from datetime import datetime

import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
import requests


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2018, 9, 12, tzinfo=pendulum.timezone('America/Los_Angeles')),
    'pool': 'metrics_pool',
    'email': ['jharris@coh.org', 'nbyers@coh.org'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

with DAG('orchestrate_metrics_qa', default_args=default_args, catchup=False, schedule_interval='0 20 * * *') as dag:

    base_url = 'https://vpxrstudio.coh.org/content/5fceaff8-8811-41ac-be8b-88aae904b2b6'
    url = f'{base_url}/nodes/'
    data = requests.get(url, verify=False).json()
    token = Variable.get('metrics_api_token')

    def prep_name(node_name):
        return node_name.lower().replace(' ', '_').replace('(', '').replace(')', '').replace('/', '')
    
    def refresh_node(node_url, api_token):
        requests.put(
            node_url,
            headers={'x-access-token': api_token},
            verify=False,
        )

    n = {}
    for node in data:
        node_type = prep_name(node['type'])
        refresh_url = f"{base_url}/refresh/{node_type}/qa/{node['id']}"
        node = f"{node_type}_{prep_name(node['name'])}"
        task = PythonOperator(
            task_id=node,
            python_callable=refresh_node,
            op_kwargs={'node_url': refresh_url, 'api_token': Variable.get('metrics_api_token')},
        )
        n[node] = task

    for node in data:
        for parent in node['parents']:
            c = f"{prep_name(node['type'])}_{prep_name(node['name'])}"
            p = f"{prep_name(parent['type'])}_{prep_name(parent['name'])}"
            n.get(p) >> n.get(c)
