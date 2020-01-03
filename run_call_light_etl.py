from datetime import datetime, timedelta

import pendulum

from airflow import DAG
from airflow.operators.mssql_operator import MsSqlOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.sensors import ExternalTaskSensor

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

dag = DAG('run_call_light_etl', default_args=default_args, catchup=False, schedule_interval='0 5 * * *')

conn_id = 'ebi_datamart'
pool_id = 'ebi_etl_pool'

deps = ExternalTaskSensor(
        external_dag_id='run_daily_census',
        external_task_id='refresh_daily_census',
        task_id='wait_for_daily_census',
        dag=dag
        )

CLT = MsSqlOperator(
        sql='EXEC EBI_CallLight_tmp_Logic;',
        task_id='call_light_tmp',
        autocommit=True,
        mssql_conn_id=conn_id,
        pool=pool_id,
        dag=dag
        )

CLC = MsSqlOperator(
        sql='EXEC EBI_CallLightUntC_Logic;',
        task_id='call_light_unt_c',
        autocommit=True,
        mssql_conn_id=conn_id,
        pool=pool_id,
        dag=dag
        )

CLCT = MsSqlOperator(
        sql='EXEC EBI_CallLightUntC_tmp_Logic;',
        task_id='call_light_unt_c_tmp',
        autocommit=True,
        mssql_conn_id=conn_id,
        pool=pool_id,
        dag=dag
        )

CLM = MsSqlOperator(
        sql='EXEC EBI_CallLight_Master_Logic;',
        task_id='call_light_master',
        autocommit=True,
        mssql_conn_id=conn_id,
        pool=pool_id,
        dag=dag
        )

CLSW = MsSqlOperator(
        sql='EXEC EBI_CallLight_Service_Words_Logic;',
        task_id='call_light_service_words',
        autocommit=True,
        mssql_conn_id=conn_id,
        pool=pool_id,
        dag=dag
        )

CLR = MsSqlOperator(
        sql='EXEC EBI_CallLight_Rounding_Logic;',
        task_id='call_light_service_rounding',
        autocommit=True,
        mssql_conn_id=conn_id,
        pool=pool_id,
        dag=dag
        )

RCLM = PythonOperator(
        task_id='refresh_ebi_call_light_master',
        python_callable=refresh_tableau_extract,
        op_kwargs={'datasource_id': '7e5432a9-854a-4d6d-801a-aea27211cb6a'},
        dag=dag
        )

RCL = PythonOperator(
        task_id='refresh_ebi_call_light',
        python_callable=refresh_tableau_extract,
        op_kwargs={'datasource_id': '082ef2af-6290-4fcf-8538-5b424043f045'},
        dag=dag
        )

RCLSW = PythonOperator(
        task_id='refresh_ebi_call_light_service_words',
        python_callable=refresh_tableau_extract,
        op_kwargs={'datasource_id': '2c42fb84-658a-45d6-b45d-ed45ba3a605d'},
        dag=dag
        )

RCLR = PythonOperator(
        task_id='refresh_ebi_call_light_rounding',
        python_callable=refresh_tableau_extract,
        op_kwargs={'datasource_id': '4d1eb306-cddc-4f83-bae5-18d5c3dd74df'},
        dag=dag
        )

deps >> CLT
deps >> CLC
CLC >> CLCT
CLT >> CLM
CLCT >> CLM
CLM >> RCLM
CLM >> RCL
CLM >> CLSW
CLSW >> RCLSW
CLSW >> CLR
CLR >> RCLR