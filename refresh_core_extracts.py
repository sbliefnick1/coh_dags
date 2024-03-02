from datetime import datetime, timedelta

import pendulum
from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator

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

with DAG('refresh_core_extracts', default_args=default_args, catchup=False, schedule_interval='0 19 * * *') as dag:

    repo = 'C:\\Users\\ebitabuser\\Documents\\ebi-automations'
    enviro = 'ebi_automations'

    run_extracts_bash = f'cd {repo} && conda activate {enviro} && refresh_priority_extracts.py'
    
    run = SSHOperator(
        ssh_conn_id='tableau_server',
        task_id='rerun_extracts',
        command=run_extracts_bash,
    )
    
    run
    
