from datetime import datetime

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
import requests
import json



default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2018, 9, 12, tzinfo=pendulum.timezone('America/Los_Angeles')),
    'email': ['jharris@coh.org', 'nbyers@coh.org'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

with DAG('get_metrics_deps', default_args=default_args, catchup=False, schedule_interval='0 1 * * *') as dag:

    base_url = 'https://vpxrstudio.coh.org/content/5fceaff8-8811-41ac-be8b-88aae904b2b6'
    url = f'{base_url}/nodes/'

    def fetch_nodes():
        resp = requests.get(
            url,
            verify=False,
        )
        if resp.status_code != 200:
            print(f'Sending GET request to {url}')
            print(resp.content)
            raise ValueError(f'GET operation failed with response')
        else:
            data = resp.json()
            json_path = '/var/nfsshare/etl_deps/metrics_deps.json'
            with open(json_path, 'w') as outfile:
                json.dump(data, outfile)

    task = PythonOperator(
        task_id='fetch_deps_to_json',
        python_callable=fetch_nodes,
        pool='metrics_pool'
    )
