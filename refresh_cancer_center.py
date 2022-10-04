from datetime import datetime, timedelta

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from auxiliary.outils import refresh_tableau_extract

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 9, 22, tzinfo=pendulum.timezone('America/Los_Angeles')),
    'concurrency': 6,
    'email': ['dscarborough@coh.org', 'sbliefnick@coh.org'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
    }

dag = DAG('refresh_cancer_center', default_args=default_args, catchup=False, schedule_interval='0 2 * * 1')

datasources = [
    {'task_id': 'refresh_cancer_center_publications',
     'datasource_id': 'd441b956-7fc8-4e2b-a2a1-1a6b483a81c6'},
    ]

for d in datasources:
    task = PythonOperator(
            task_id=d['task_id'],
            python_callable=refresh_tableau_extract,
            op_kwargs={'datasource_id': d['datasource_id']},
            dag=dag
            )

    task
