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

with DAG('refresh_ctca_salesforce', default_args=default_args, catchup=False, schedule_interval='50 3 * * *') as dag:

    repo = 'C:\\Users\\ebitabuser\\Documents\\ebi-data-engineering'
    snwflk_repo = f'{repo}\\snowflake'
    enviro = 'ebi_data_engineering'

    git_pull_bash = f'cd {repo} && git pull'
    sf_coh_appts_bash = f'cd {snwflk_repo} && conda activate {enviro} && python salesforce_coh_appointments.py'

    git = SSHOperator(
        ssh_conn_id='tableau_server',
        task_id='git_pull_latest',
        command=git_pull_bash,
    )

    sf_appts = SSHOperator(
        ssh_conn_id='tableau_server',
        task_id='sf_coh_appts',
        command=sf_coh_appts_bash,
    )
    
    git >> sf_appts
