from datetime import datetime, timedelta

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from auxiliary.outils import refresh_tableau_extract

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 9, 6, tzinfo=pendulum.timezone('America/Los_Angeles')),
    'email': ['sbliefnick@coh.org', 'hcarlson@coh.org'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
    }

dag = DAG('refresh_computrition', default_args=default_args, catchup=False, schedule_interval='0 11 * * *')

rc = PythonOperator(
        task_id='refresh_custom_sql_computrition_extract',
        python_callable=refresh_tableau_extract,
        op_kwargs={'datasource_id': 'd30a733f-ffed-4e47-8ffe-e20cdcecc8ca'},
        dag=dag
        )
