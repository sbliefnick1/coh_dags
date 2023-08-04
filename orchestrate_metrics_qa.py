from datetime import datetime

import pendulum
from airflow import DAG
from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator
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

with DAG('orchestrate_metrics_qa', default_args=default_args, catchup=False, schedule_interval='0 20 * * *') as dag:

    conn_id = 'ebi_datamart'
    pool_id = 'ebi_etl_pool'
    
    json_path = '/var/nfsshare/etl_deps/metrics_deps.json'
    with open(json_path, 'r') as infile:
        data = json.load(infile)

    def prep_name(node_name):
        return node_name.lower().replace(' ', '_').replace('(', '').replace(')', '').replace('/', '')

    n = {}
    for node in data:

        node_type = prep_name(node['type'])
        task_name = prep_name(node['name'])
        node_task_id = f"{node_type}_{task_name}"
        node_sql = node['sql']

        if node_type == 'base':
            weight = 3
        elif node_type == 'instance':
            weight = 2
        else:
            weight = 1
        
        task = MsSqlOperator(
            sql=node_sql,
            task_id=node_task_id,
            autocommit=True,
            mssql_conn_id=conn_id,
            pool=pool_id,
        )
        n[node_task_id] = task

    for node in data:
        for parent in node['parents']:
            c = f"{prep_name(node['type'])}_{prep_name(node['name'])}"
            p = f"{prep_name(parent['type'])}_{prep_name(parent['name'])}"
            n.get(p) >> n.get(c)
