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
    bases = list(set([d[0].replace(".sql", "").replace(" ", "_") for d in both]))
    colls = list(set([d[1].replace(" ", "_") for d in both]))

    bs = {}
    for base in bases:
        task = DummyOperator(task_id = base)
        bs[base] = task

    cs = {}
    for coll in colls:
        task = DummyOperator(task_id = coll)
        cs[coll] = task

    print(bs)
    for d in both:
        f = d[0].replace(".sql", "").replace(" ", "_")
        l = d[1].replace(" ", "_")
        bs.get(f) >> cs.get(l)
