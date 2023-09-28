from datetime import datetime, timedelta

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator
from auxiliary.outils import refresh_tableau_extract

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

with DAG('refresh_total_charges', default_args=default_args, catchup=False, schedule_interval='0 17 * * *') as dag:

    conn_id = 'ebi_datamart'
    pool_id = 'ebi_etl_pool'

    rvus = PythonOperator(
        task_id='refresh_rvu_extract',
        python_callable=refresh_tableau_extract,
        op_kwargs={'datasource_id': 'c08148a1-cf27-48df-8c8f-fc29f2c77c12'},
        dag=dag
    )

    rvus
