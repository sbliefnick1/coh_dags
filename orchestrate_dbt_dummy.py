from datetime import datetime

import pendulum
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.utils.task_group import TaskGroup
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

with DAG('orchestrate_dbt_dummy', default_args=default_args, catchup=False, schedule_interval='0 1 * * *') as dag:

    url = 'http://build.coh.org:37000/manifest.json'
    manifest = requests.get(url).json()
    manifest_nodes = manifest['nodes']
    manifest_sources = manifest['sources']
    child_map = manifest['child_map']
    parent_map = manifest['parent_map']

    ops = {}
    for node in manifest_nodes.keys():
        if node.split('.')[0] == 'model':
            with TaskGroup(node, tooltip=f"Tasks for {node}") as tg:
                node_name = node.split('.')[2]
                task = DummyOperator(task_id = f'run-{node_name}')
                for test in child_map[node]:
                    if test.split('.')[0] == 'test':
                        test_name = test.split('.')[2][0:100]
                        test_task = DummyOperator(task_id = f'test-{test_name}')
                        task >> test_task
                ops[node] = tg

    sources = set([s.split('.')[2] for s in manifest_sources.keys()])
    srcs = {}
    for src in sources:
        task = DummyOperator(task_id = f'sensor-{src}')
        srcs[src] = task

    for parent in child_map.keys():
        if parent.split('.')[0] == 'model':
            for child in child_map[parent]:
                if child.split('.')[0] == 'model':
                    ops[parent] >> ops[child]

    for node in parent_map.keys():
        if node.split('.')[0] == 'model':
            for parent in parent_map[node]:
                parent_split = parent.split('.')
                if parent_split[0] == 'source':
                    src_sys = parent_split[2]
                    srcs[src_sys] >> ops[node]
