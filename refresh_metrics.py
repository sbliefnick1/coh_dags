from datetime import datetime, timedelta

import pendulum
from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.operators.python import PythonOperator
from auxiliary.outils import get_json_secret

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

dag = DAG('refresh_metrics', default_args=default_args, catchup=False, schedule_interval='35 5 * * *')

ebi = get_json_secret('ebi_db_conn')['db_connections']['fi_dm_ebi']

git_pull_latest = 'cd C:\\Anaconda\\ETL\\metrics && git pull'
propagate_base_sql_views = 'cd C:\\Anaconda\\ETL\\metrics\\collections && conda activate metrics && python base_sql_propagation.py'



gp = SSHOperator(ssh_conn_id='tableau_server',
                 task_id='git_pull_latest',
                 command=git_pull_latest,
                 dag=dag,
                 priority_weight=100)


pbsv = SSHOperator(ssh_conn_id='tableau_server',
                   task_id='propagate_base_sql_views',
                   command=propagate_base_sql_views,
                   dag=dag,
                   priority_weight=100)

gp >> pbsv
