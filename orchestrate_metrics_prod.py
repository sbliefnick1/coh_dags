from datetime import datetime

import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
import requests
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

with DAG('orchestrate_metrics_prod', default_args=default_args, catchup=False, schedule_interval='0 11 * * *') as dag:

    base_url = 'https://vpxrstudio.coh.org/content/5fceaff8-8811-41ac-be8b-88aae904b2b6'
    token = Variable.get('metrics_api_token')
    
    def refresh_node(node_url, api_token):
        resp = requests.put(
            node_url,
            headers={'x-access-token': api_token},
            verify=False,
        )
        print(f'Sending PUT request to {node_url}')
        print(resp.content)
        if resp.status_code != 200:
            raise ValueError(f'PUT operation failed with response')
        time.sleep(10)


    base_run = PythonOperator(
        task_id='run_base_metrics',
        python_callable=refresh_node,
        op_kwargs={
            'node_url': f'{base_url}/refresh/base/prod/all',
            'api_token': token
        },
        pool='metrics_pool',
    )

    instance_run = PythonOperator(
        task_id='run_instance_metrics',
        python_callable=refresh_node,
        op_kwargs={
            'node_url': f'{base_url}/refresh/instance/prod/all',
            'api_token': token
        },
        pool='metrics_pool',
    )

    collection_run = PythonOperator(
        task_id='run_collection_metrics',
        python_callable=refresh_node,
        op_kwargs={
            'node_url': f'{base_url}/refresh/collection/prod/all',
            'api_token': token
        },
        pool='metrics_pool',
    )

    base_run >> instance_run >> collection_run
