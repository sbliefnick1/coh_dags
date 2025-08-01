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

    repo = r'C:\Users\ebitabuser\Documents\ebi-data-engineering\supplemental'
    enviro = 'ebi_data_engineering'
    python_exe = rf'C:\Users\ebitabuser\AppData\Local\Miniconda3\envs\{enviro}\python.exe'
    prefix = f'cd {repo} && "{python_exe}"'

    rvu = SSHOperator(
        ssh_conn_id='ebi_etl_server',
        task_id='rvus_to_snowflake',
        command=f'{prefix} rvus.py',
    )

    rvu
