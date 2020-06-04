from datetime import timedelta, datetime

import pendulum

from airflow import DAG
from airflow.contrib.operators.ssh_operator import SSHOperator

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

dag = DAG('hr_tableau_security_sync', default_args=default_args, catchup=False, schedule_interval='0 21 * * *')

hr_bash = 'cd C:\\Anaconda\\ETL\\tableau && python hr_security.py'
epic_bash = 'cd C:\\Anaconda\\ETL\\tableau && python TableauEpicSecuritySync.py'
telehlth_bash = 'cd C:\\Anaconda\\ETL\\tableau && python TableauOutofStateApptProvs.py'

t1 = SSHOperator(ssh_conn_id='tableau_server',
                 task_id='Sync_HR_Users_And_Groups',
                 command=hr_bash,
                 dag=dag)

t2 = SSHOperator(ssh_conn_id='tableau_server',
                 task_id='Sync_Epic_Users_And_Groups',
                 command=epic_bash,
                 dag=dag)

t3 = SSHOperator(ssh_conn_id='tableau_server',
                 task_id='Sync_Telehealth_Providers',
                 command=telehlth_bash,
                 dag=dag)