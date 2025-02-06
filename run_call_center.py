from datetime import datetime, timedelta

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.sql import SqlSensor
from auxiliary.outils import refresh_tableau_extract


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2018, 9, 12, tzinfo=pendulum.timezone('America/Los_Angeles')),
    'email': ['jharris@coh.org'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
    }

dag = DAG('run_call_center', default_args=default_args, catchup=False, schedule_interval='40 5 * * *')


SURV = PythonOperator(
        task_id='refresh_cisco_call_center_surveys',
        python_callable=refresh_tableau_extract,
        op_kwargs={'datasource_id': '1e51f282-6346-473f-b4ca-2fa2bff18d6f'},
        dag=dag
        )

CAB = PythonOperator(
        task_id='refresh_calabrio_agents_productivity',
        python_callable=refresh_tableau_extract,
        op_kwargs={'datasource_id': '8a77dd16-5b63-4f25-8b0a-dc59708bad4f'},
        dag=dag
        )

CAES = PythonOperator(
        task_id='refresh_calabrio_agents_evaluation_scores',
        python_callable=refresh_tableau_extract,
        op_kwargs={'datasource_id': 'b07d75e4-1abf-4085-8ace-f0f9f85b7376'},
        dag=dag
        )

CLU = PythonOperator(
        task_id='refresh_calabrio_license_usage',
        python_callable=refresh_tableau_extract,
        op_kwargs={'datasource_id': 'fc30c9ee-e7be-40fa-a596-6a3f75884be1'},
        dag=dag
        )

SURV
CAB
CAES
