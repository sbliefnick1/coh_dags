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
    repo = 'C:\\Users\\ebitabuser\\Documents\\ebi-data-engineering'
    auto_repo = f'{repo}\\automations'
    enviro = 'ebi_data_engineering'

    hr_bash = 'cd C:\\Anaconda\\ETL\\tableau && conda activate foundation && python hr_security.py'
    unlicense_bash = f'cd {auto_repo} && conda activate {enviro} && python tableau_unlicense_users.py'
    mf_sched = 'cd C:\\Anaconda\\ETL\\tableau && python MF_Schedulers_Security.py'

    t1 = SSHOperator(ssh_conn_id='tableau_server',
                    task_id='Sync_HR_Users_And_Groups',
                    command=hr_bash,
                    dag=dag)

    t3 = SSHOperator(ssh_conn_id='tableau_server',
                    task_id='Unlicense_Tableau_Users',
                    command=unlicense_bash,
                    dag=dag)

    t4 = SSHOperator(ssh_conn_id='tableau_server',
                    task_id='MF_Schedulers_Security',
                    command=mf_sched,
                    dag=dag)
