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

dag = DAG('refresh_scm', default_args=default_args, catchup=False, schedule_interval='0 16 * * *')

conn_id = 'ebi_datamart'
pool_id = 'ebi_etl_pool'

si = MsSqlOperator(
        sql='EXEC EBI_SCM_Items_Logic;',
        task_id='load_scm_items',
        autocommit=True,
        mssql_conn_id=conn_id,
        pool=pool_id,
        dag=dag
        )

scmi = PythonOperator(
        task_id='refresh_scm_inventory',
        python_callable=refresh_tableau_extract,
        op_kwargs={'datasource_id': 'c5c47779-a321-48ee-a12f-1f4c933f26c6'},
        dag=dag
        )

scmibu = PythonOperator(
        task_id='refresh_scm_inventory_business_units',
        python_callable=refresh_tableau_extract,
        op_kwargs={'datasource_id': '1da002a9-25fd-4139-b4d1-e3244ef919fb'},
        dag=dag
        )

scmta = PythonOperator(
        task_id='refresh_scm_transport_activity',
        python_callable=refresh_tableau_extract,
        op_kwargs={'datasource_id': 'fee39e73-5d2d-480d-9e40-be594d08ed7a'},
        dag=dag
        )


si >> scmi
si >> scmibu
si >> scmta
