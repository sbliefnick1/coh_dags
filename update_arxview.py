from datetime import timedelta, datetime

import pendulum

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.ssh_operator import SSHOperator

from auxiliary.outils import refresh_tableau_extract

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['sbliefnick@coh.org', 'jharris@coh.org'],
    'email_on_failure': True,
    'email_on_retry': False,
    'start_date': datetime(2019, 4, 2, tzinfo=pendulum.timezone('America/Los_Angeles')),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
    }

dag = DAG('update_arxview', default_args=default_args, catchup=False, schedule_interval='0 9 * * *')

t1_bash = 'activate arxview && python C:\\Anaconda\\ETL\\arxview\\update_arxview.py'

t1 = SSHOperator(ssh_conn_id='tableau_server',
                 task_id='run_get_arxview',
                 command=t1_bash,
                 dag=dag)

t2 = PythonOperator(task_id='refresh_arxview_arrays',
                    python_callable=refresh_tableau_extract,
                    op_kwargs={'datasource_id': '7d239d58-aea8-4dbb-bb98-cac214f1a021'},
                    dag=dag)

t1 >> t2
