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
    'Claro Healthcare Data Month -tbl-202001-202004.xlsx',
    'Claro Healthcare Data Month -tbl-202005-202008.xlsx',
    'Claro Healthcare Data Month -tbl-202009-202012.xlsx',
    'Claro Healthcare Data Month -tbl-202101-202104.xlsx',
    'Claro Healthcare Data Month -tbl-202105-202108.xlsx',
    'Claro Healthcare Data Month -tbl-202109-202112.xlsx',
    'Claro Healthcare Data Month -tbl-202201-202204.xlsx',
    'Claro Healthcare Data Month -tbl-202205-202208.xlsx',
    ]

for file in files:
    sftp = SFTPOperator(task_id=file,
                        ssh_conn_id='claro_sftp',
                        local_filepath=f'/var/nfsshare/files/claro/{file}',
                        remote_filepath=f'/{file}',
                        create_intermediate_dirs=True,
                        dag=dag)
    sftp
