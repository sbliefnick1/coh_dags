from datetime import timedelta, datetime

import pendulum
from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['jharris@coh.org'],
    'email_on_failure': True,
    'email_on_retry': False,
    'start_date': datetime(2019, 3, 6, tzinfo=pendulum.timezone('America/Los_Angeles')),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    }

with DAG('hr_tableau_security_sync', default_args=default_args, catchup=False, schedule_interval='0 21 * * *') as dag:

    repo = r'C:\Users\ebitabuser\Documents\ebi-data-engineering\automations'
    enviro = 'ebi_data_engineering'
    python_exe = rf'C:\Users\ebitabuser\AppData\Local\Miniconda3\envs\{enviro}\python.exe'
    prefix = f'cd {repo} && "{python_exe}"'

    mf_sec = SSHOperator(
        ssh_conn_id='ebi_etl_server',
        task_id='MF_Tableau_Security',
        command=f'{prefix} mf_tableau_security.py',
    )

    mf_sec
