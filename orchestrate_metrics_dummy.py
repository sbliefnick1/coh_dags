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

    url = 'http://git.coh.org:7990/projects/EBI/repos/metrics/raw/artifacts/dag_helper.json?at=refs%2Fheads%2Fplatform_agnostic'
    manifest = requests.get(url).json()
    deps = requests.get(url).json()['base_to_collection_dependencies']
    both = [d.split(" >> ") for d in deps]

    def clean_name(n):
        return n.replace(".sql", "").replace(" ", "_")

    bases = list(set([f'base-{clean_name(d[0])}' for d in both]))
    colls = list(set([f'collection-{clean_name(d[1])}' for d in both]))

    bs = {}
    for base in bases:
        task = DummyOperator(task_id=base)
        bs[base] = task

    cs = {}
    for coll in colls:
        task = DummyOperator(task_id=coll)
        cs[coll] = task

    print(bs)
    for d in both:
        f = f'base-{clean_name(d[0])}'
        l = f'collection-{clean_name(d[1])}'
        bs.get(f) >> cs.get(l)
