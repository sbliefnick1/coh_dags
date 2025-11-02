from datetime import datetime, timedelta

import pendulum
from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator
from airflow.operators.python import PythonOperator
from auxiliary.outils import refresh_tableau_extract

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 12, 12, tzinfo=pendulum.timezone('America/Los_Angeles')),
    'email': ['jharris@coh.org'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
    }

with DAG('run_clinical_finance_tasks', default_args=default_args, catchup=False, schedule_interval='0 13 * * *') as dag:

    conn_id = 'ebi_datamart'
    pool_id = 'ebi_etl_pool'

    ebi_sql = """
        exec EBI_Enterprise_Labor_Logic;
    """

    repo = r'C:\Users\ebitabuser\Documents\ebi-data-engineering\clinical_finance'
    enviro = 'ebi_data_engineering'
    python_exe = rf'C:\Users\ebitabuser\AppData\Local\Miniconda3\envs\{enviro}\python.exe'
    prefix = f'cd {repo} && "{python_exe}"'

    ebi = MsSqlOperator(
        sql=ebi_sql,
        task_id='refresh_labor_table_in_ebi',
        autocommit=True,
        mssql_conn_id=conn_id,
        pool=pool_id,
        dag=dag,
    )
