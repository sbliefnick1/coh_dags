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

    maps = SSHOperator(
        ssh_conn_id='ebi_etl_server',
        task_id='refresh_mapping_tables',
        command=f'{prefix} mappings_to_ebi.py',
        dag=dag,
    )

    ebi = MsSqlOperator(
        sql=ebi_sql,
        task_id='refresh_labor_table_in_ebi',
        autocommit=True,
        mssql_conn_id=conn_id,
        pool=pool_id,
        dag=dag,
    )

    tab = PythonOperator(
        task_id='refresh_labor_table_in_tableau',
        python_callable=refresh_tableau_extract,
        op_kwargs={'datasource_id': '149fbbfa-b146-454e-be88-f7c365ccafbe'},
        dag=dag,
    )

    fter = PythonOperator(
        task_id='refresh_labor_fte',
        python_callable=refresh_tableau_extract,
        op_kwargs={'datasource_id': 'DE1BE98A-B246-4317-9223-97C2F533EB43'},
        dag=dag,
    )

    maps
    ebi >> tab
    ebi >> fter
