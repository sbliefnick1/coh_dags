from datetime import datetime, timedelta

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator
from auxiliary.outils import get_json_secret
import tableauserverclient as TSC

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

dag = DAG('refresh_qrrm_data', default_args=default_args, catchup=False, concurrency=2, schedule_interval='35 5 * * *')

conn_id = 'qrrm_datamart'

ebi = get_json_secret('ebi_db_conn')['db_connections']['fi_dm_ebi']
auth = TSC.TableauAuth(ebi['user'].split(sep='\\')[1], ebi['password'])
server = TSC.Server('https://ebi.coh.org', use_server_version=True)

def refresh_workbook_data(tableau_server, tableau_authentication, workbook_luid):
        with server.auth.sign_in(tableau_authentication):
                server.workbooks.refresh(workbook_luid)

cp = MsSqlOperator(
        sql='EXEC Clarity_COVID19_SP;',
        task_id='covid_proc',
        autocommit=True,
        mssql_conn_id=conn_id,
        dag=dag
        )

ce = PythonOperator(
        task_id='covid_extract',
        python_callable=refresh_workbook_data,
        op_kwargs={'tableau_server': server, 'tableau_authentication': auth, 'workbook_luid': '2fd3f851-37b9-45d1-bedc-0d7cdacdf888'},
        dag=dag
        )

cp >> ce
