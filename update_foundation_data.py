from datetime import timedelta, datetime

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from auxiliary.outils import refresh_tableau_extract

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

dag = DAG('update_foundation_data', default_args=default_args, catchup=False, schedule_interval='00 21 * * *')

t1_bash = 'cd C:\\Anaconda\\ETL\\foundation && python DSS_D_Data.py'
t2_bash = 'cd C:\\Anaconda\\ETL\\foundation && python LU_Physicians.py'
t4_bash = 'cd C:\\Anaconda\\ETL\\misc_etl && python CovidWaiverData.py'

t1 = SSHOperator(ssh_conn_id='tableau_server',
                 task_id='refresh_dss_d_data',
                 command=t1_bash,
                 dag=dag)

t3 = PythonOperator(
        task_id='refresh_rvu_extract',
        python_callable=refresh_tableau_extract,
        op_kwargs={'datasource_id': 'c08148a1-cf27-48df-8c8f-fc29f2c77c12'},
        dag=dag
        )

t4 = SSHOperator(ssh_conn_id='tableau_server',
                 task_id='refresh_covid_waiver',
                 command=t4_bash,
                 dag=dag)

t1 >> t3
t4
