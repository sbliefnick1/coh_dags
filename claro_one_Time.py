from datetime import datetime, timedelta

import pendulum
from airflow import DAG
from airflow.providers.sftp.operators.sftp import SFTPOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 9, 6, tzinfo=pendulum.timezone('America/Los_Angeles')),
    'email': ['sbliefnick@coh.org'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
    }

dag = DAG('claro_one_time', default_args=default_args, catchup=False, schedule_interval=None)

files = [
    'Clarity_PRD_Report_c_Patient_Account_File.csv',
    ]

for file in files:
    sftp = SFTPOperator(task_id=file.replace(' ', ''),
                        ssh_conn_id='claro_sftp',
                        local_filepath=f'/var/nfsshare/files/claro/{file}',
                        remote_filepath=f'/{file}',
                        create_intermediate_dirs=True,
                        dag=dag)
    sftp
