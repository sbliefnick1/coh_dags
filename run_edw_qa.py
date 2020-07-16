from datetime import datetime, timedelta

import pendulum

from airflow import DAG
from airflow.operators.mssql_operator import MsSqlOperator
from airflow.operators.python_operator import PythonOperator

from auxiliary.outils import refresh_tableau_extract

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 7, 15, tzinfo=pendulum.timezone('America/Los_Angeles')),
    'email': ['jharris@coh.org'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
    }

dag = DAG('run_edw_qa', default_args=default_args, catchup=False, schedule_interval='30 6 * * *')

conn_id = 'ebi_datamart'
pool_id = 'ebi_etl_pool'

scp = MsSqlOperator(
        sql='EXEC EBI_SEM_Census_QA_Logic;',
        task_id='sem_census_qa',
        autocommit=True,
        mssql_conn_id=conn_id,
        pool=pool_id,
        dag=dag
        )

sce = PythonOperator(
        task_id='refresh_scm_census_qa_extract',
        python_callable=refresh_tableau_extract,
        op_kwargs={'datasource_id': 'df1da0b7-7b06-4e4e-bd8e-0dfd5c3a42a1'},
        dag=dag
        )

scp >> sce