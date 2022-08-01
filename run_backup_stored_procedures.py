from datetime import datetime, timedelta

import pendulum
from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 5, 20, tzinfo=pendulum.timezone('America/Los_Angeles')),
    'email': ['sbliefnick@coh.org', 'jharris@coh.org'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
    }

dag = DAG('run_backup_stored_procedures', default_args=default_args, catchup=False, schedule_interval='0 21 * * *')

dictionary_bash = 'cd C:\\Anaconda\\ETL\\misc_etl && conda activate foundation && python ebi_dictionary.py'
changes_version_bash = 'cd C:\\Anaconda\\ETL\\fi_dm_ebi && conda activate foundation && python version_stored_procedures.py'
commit_version_bash = 'cd C:\\Anaconda\\ETL\\fi_dm_ebi\\ebi-stored-procedures && git add -A && git diff --quiet && git diff --staged --quiet || git commit -am "add changes"'
push_version_bash = 'cd C:\\Anaconda\\ETL\\fi_dm_ebi\\ebi-stored-procedures && git push'

d = SSHOperator(
    ssh_conn_id='tableau_server',
    task_id='refresh_dictionary',
    command=dictionary_bash,
    dag=dag
)

cv = SSHOperator(
    ssh_conn_id='tableau_server',
    task_id='add_changes_to_versioning_folder',
    command=changes_version_bash,
    dag=dag
)

cmv = SSHOperator(
    ssh_conn_id='tableau_server',
    task_id='commit_versioning_changes_to_git',
    command=commit_version_bash,
    dag=dag
)

pv = SSHOperator(
    ssh_conn_id='tableau_server',
    task_id='push_versioning_changes_to_git',
    command=push_version_bash,
    dag=dag
)

cv >> cmv >> pv
