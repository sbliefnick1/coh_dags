from datetime import datetime, timedelta

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from auxiliary.outils import refresh_tableau_extract

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2018, 9, 12, tzinfo=pendulum.timezone('America/Los_Angeles')),
    'concurrency': 6,
    'email': ['jharris@coh.org', 'sbliefnick@coh.org'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
    }

dag = DAG('run_clarity_direct_refreshes', default_args=default_args, catchup=False, schedule_interval='00 5 * * *')

hbs = PythonOperator(
        task_id='refresh_hospital_billing_summary',
        python_callable=refresh_tableau_extract,
        op_kwargs={'datasource_id': '97774949-8372-4128-9730-3C9432C0305E'},
        dag=dag
        )

hbs
