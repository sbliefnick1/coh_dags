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
    'email': ['jharris@coh.org'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
    }

dag = DAG('run_call_center', default_args=default_args, catchup=False, schedule_interval='40 5 * * *')

deps = ExternalTaskSensor(
        external_dag_id='run_daily_census',
        external_task_id='refresh_daily_census',
        task_id='wait_for_daily_census',
        dag=dag
        )

ACC = PythonOperator(
        task_id='refresh_avaya_call_center',
        python_callable=refresh_tableau_extract,
        op_kwargs={'datasource_id': '6EE81264-DFE1-4318-8397-9EC853DF7085'},
        dag=dag
        )

ACCA = PythonOperator(
        task_id='refresh_avaya_call_center_agents',
        python_callable=refresh_tableau_extract,
        op_kwargs={'datasource_id': '082E7473-5803-4681-A847-3ADAB4B1E8E7'},
        dag=dag
        )

AAGP = PythonOperator(
        task_id='refresh_avaya_agent_group_planner',
        python_callable=refresh_tableau_extract,
        op_kwargs={'datasource_id': 'B4F5407D-36E1-473D-87F1-D5B6AE0A9527'},
        dag=dag
        )

AAGPH = PythonOperator(
        task_id='refresh_avaya_agent_group_planner_hour',
        python_callable=refresh_tableau_extract,
        op_kwargs={'datasource_id': '8B2C21BF-9EFC-491A-BC55-855F1B065393'},
        dag=dag
        )

AVV = PythonOperator(
        task_id='refresh_avaya_vdn_vector',
        python_callable=refresh_tableau_extract,
        op_kwargs={'datasource_id': '7f9bdba7-b64e-4225-a777-490dc14ffb69'},
        dag=dag
        )

deps >> ACC
deps >> ACCA
deps >> AAGP
deps >> AAGPH
deps >> AVV
