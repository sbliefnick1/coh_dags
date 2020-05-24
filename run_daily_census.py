from datetime import datetime, timedelta

import pendulum

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.sensors import ExternalTaskSensor

from auxiliary.outils import refresh_tableau_extract

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2018, 9, 12, tzinfo=pendulum.timezone('America/Los_Angeles')),
    'email': ['jharris@coh.org', 'ddeaville@coh.org'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
    }

dag = DAG('run_daily_census', default_args=default_args, catchup=False, schedule_interval='35 5 * * *')

#deps = ExternalTaskSensor(
#        external_dag_id='get_etl_deps',
#        external_task_id='query_and_save_deps',
#        task_id='wait_for_dependencies_file',
#        dag=dag
#        )

RDC = PythonOperator(
        task_id='refresh_daily_census',
        python_callable=refresh_tableau_extract,
        op_kwargs={'datasource_id': '86a56fc4-d842-40ad-8898-2249e302c88d'},
        dag=dag
        )

#deps >> 
RDC