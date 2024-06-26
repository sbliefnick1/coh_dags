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

with DAG('refresh_ebi_snowflake_data', default_args=default_args, catchup=False, schedule_interval='0 20 * * *') as dag:

    repo = 'C:\\Users\\ebitabuser\\Documents\\ebi-data-engineering'
    supp_repo = f'{repo}\\supplemental'
    enviro = 'ebi_data_engineering'

    git_pull_bash = f'cd {repo} && git pull'
    rvu_bash = f'cd {supp_repo} && conda activate {enviro} && python rvus.py'

    git = SSHOperator(
        ssh_conn_id='tableau_server',
        task_id='git_pull_latest',
        command=git_pull_bash,
    )

    rvu = SSHOperator(
        ssh_conn_id='tableau_server',
        task_id='rvus_to_snowflake',
        command=rvu_bash,
    )
    
    git >> rvu
