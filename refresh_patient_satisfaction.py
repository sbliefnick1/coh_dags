from datetime import datetime, timedelta

import pendulum

from airflow import DAG
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.operators.mssql_operator import MsSqlOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2018, 9, 12, tzinfo=pendulum.timezone('America/Los_Angeles')),
    'email': ['jharris@coh.org'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
    }

dag = DAG('refresh_patient_satisfaction', default_args=default_args, catchup=False, schedule_interval='0 18 * * *')

conn_id = 'ebi_datamart'
pool_id = 'ebi_etl_pool'

results_bash = 'cd C:\\Anaconda\\ETL\\patient_sat && python patient_sat_results.py'
ranks_bash = 'cd C:\\Anaconda\\ETL\\patient_sat && python patient_sat_percentile.py'
results_bash_new = 'cd C:\\Anaconda\\ETL\\patient_sat && python ResponseToDB1.py'
ranks_bash_new = 'cd C:\\Anaconda\\ETL\\patient_sat && python RanksToDB1.py'

red = MsSqlOperator(
        sql='EXEC EBI_PressGaney_Results_Clarity_Logic;',
        task_id='refresh_edw_data',
        autocommit=True,
        mssql_conn_id=conn_id,
        pool=pool_id,
        dag=dag
        )

crd = SSHOperator(ssh_conn_id='tableau_server',
                  task_id='copy_results_to_db1',
                  command=results_bash,
                  dag=dag)

crk = SSHOperator(ssh_conn_id='tableau_server',
                  task_id='copy_ranks_to_db1',
                  command=ranks_bash,
                  dag=dag)

crdn = SSHOperator(ssh_conn_id='tableau_server',
                   task_id='copy_results_to_db1_new',
                   command=results_bash_new,
                   dag=dag)

crkn = SSHOperator(ssh_conn_id='tableau_server',
                   task_id='copy_ranks_to_db1_new',
                   command=ranks_bash_new,
                   dag=dag)

red >> crd >> crk
crdn >> crkn
