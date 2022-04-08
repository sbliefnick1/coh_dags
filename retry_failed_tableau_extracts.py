from datetime import datetime, timedelta

import pendulum
from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 12, 12, tzinfo=pendulum.timezone('America/Los_Angeles')),
    'email': ['jharris@coh.org'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
    }

dag = DAG('retry_failed_tableau_extracts', default_args=default_args, catchup=False, schedule_interval='0 15 * * *')

refresh_bash = 'cd C:\\Anaconda\\ETL\\tableau && python run_failed_extracts.py'

r = SSHOperator(ssh_conn_id='tableau_server',
                task_id='find_and_rerun_failed_extracts',
                command=refresh_bash,
                dag=dag)
