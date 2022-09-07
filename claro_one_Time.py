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

sftp1 = SFTPOperator(task_id='202101-202104',
                     ssh_conn_id='claro_sftp',
                     local_filepath=f'/var/nfsshare/files/claro/Claro Healthcare Data Flat Month-202101-202104.xlsx',
                     remote_filepath=f'/Claro Healthcare Data Flat Month-202101-202104.xlsx',
                     create_intermediate_dirs=True,
                     dag=dag)

sftp2 = SFTPOperator(task_id='202105-202108',
                     ssh_conn_id='claro_sftp',
                     local_filepath=f'/var/nfsshare/files/claro/Claro Healthcare Data Flat Month-202105-202108.xlsx',
                     remote_filepath=f'/Claro Healthcare Data Flat Month-202105-202108.xlsx',
                     create_intermediate_dirs=True,
                     dag=dag)

sftp3 = SFTPOperator(task_id='202109-202112',
                     ssh_conn_id='claro_sftp',
                     local_filepath=f'/var/nfsshare/files/claro/Claro Healthcare Data Flat Month-202109-202112.xlsx',
                     remote_filepath=f'/Claro Healthcare Data Flat Month-202109-202112.xlsx',
                     create_intermediate_dirs=True,
                     dag=dag)

sftp4 = SFTPOperator(task_id='202201-202204',
                     ssh_conn_id='claro_sftp',
                     local_filepath=f'/var/nfsshare/files/claro/Claro Healthcare Data Flat Month-202201-202204.xlsx',
                     remote_filepath=f'/Claro Healthcare Data Flat Month-202201-202204.xlsx',
                     create_intermediate_dirs=True,
                     dag=dag)

sftp5 = SFTPOperator(task_id='202205-202208',
                     ssh_conn_id='claro_sftp',
                     local_filepath=f'/var/nfsshare/files/claro/Claro Healthcare Data Flat Month-202205-202208.xlsx',
                     remote_filepath=f'/Claro Healthcare Data Flat Month-202205-202208.xlsx',
                     create_intermediate_dirs=True,
                     dag=dag)
