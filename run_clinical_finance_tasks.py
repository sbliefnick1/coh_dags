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

dag = DAG('run_clinical_finance_tasks', default_args=default_args, catchup=False, schedule_interval='35 5 * * *')

refresh_maps_bash = 'cd C:\\Anaconda\\ETL\\clinical_finance\\cfin_maps_to_ebi.py'

m = SSHOperator(ssh_conn_id='tableau_server',
                task_id='refresh_mapping_tables',
                command=refresh_maps_bash,
                dag=dag)
