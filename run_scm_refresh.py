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

scws = PythonOperator(
        task_id='refresh_scm_cspt_warehouse_stock',
        python_callable=refresh_tableau_extract,
        op_kwargs={'datasource_id': '51db78e8-b319-4b8d-9c59-b4758cefdd9b'},
        dag=dag
        )

scmi = PythonOperator(
        task_id='refresh_scm_inventory',
        python_callable=refresh_tableau_extract,
        op_kwargs={'datasource_id': 'c5c47779-a321-48ee-a12f-1f4c933f26c6'},
        dag=dag
        )

scmiu = PythonOperator(
        task_id='refresh_scm_inventory_usage',
        python_callable=refresh_tableau_extract,
        op_kwargs={'datasource_id': '9737e41f-1e28-444a-8f70-0d9ee47569ec'},
        dag=dag
        )

scmibu = PythonOperator(
        task_id='refresh_scm_inventory_business_units',
        python_callable=refresh_tableau_extract,
        op_kwargs={'datasource_id': '1da002a9-25fd-4139-b4d1-e3244ef919fb'},
        dag=dag
        )

scmic = PythonOperator(
        task_id='refresh_scm_inventory_count',
        python_callable=refresh_tableau_extract,
        op_kwargs={'datasource_id': 'f046bc66-4b42-47f7-b074-243b48c49d06'},
        dag=dag
        )

scmta = PythonOperator(
        task_id='refresh_scm_transport_activity',
        python_callable=refresh_tableau_extract,
        op_kwargs={'datasource_id': 'fee39e73-5d2d-480d-9e40-be594d08ed7a'},
        dag=dag
        )

scmmmpa = PythonOperator(
        task_id='refresh_scm_materials_management_price_analysis',
        python_callable=refresh_tableau_extract,
        op_kwargs={'datasource_id': '1ba18b72-6aea-4734-a49c-3e64b9849643'},
        dag=dag
        )

scmpar = PythonOperator(
        task_id='refresh_scm_par_analysis',
        python_callable=refresh_tableau_extract,
        op_kwargs={'datasource_id': '638bac72-6b6e-4400-998e-53c3657cb7fe'},
        dag=dag
        )


si >> scws
si >> scmi
si >> scmiu
si >> scmibu
si >> scmic
si >> scmta
si >> scmmmpa
si >> scmpar
