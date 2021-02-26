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

dag = DAG('refresh_npsr_project', default_args=default_args, catchup=False, schedule_interval='0 8 * * *')

refresh_bash = 'cd C:\\Anaconda\\ETL\\npsr && python full_data_refresh.py'

r = SSHOperator(ssh_conn_id='tableau_server',
                task_id='refresh_data',
                command=refresh_bash,
                dag=dag)