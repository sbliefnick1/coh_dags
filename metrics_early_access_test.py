from datetime import datetime

import pendulum
from airflow import DAG
from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator
from auxiliary.outils import get_json_secret


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2018, 9, 12, tzinfo=pendulum.timezone('America/Los_Angeles')),
    'email': ['jharris@coh.org', 'nbyers@coh.org'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
}

with DAG('metrics_early_access_test', default_args=default_args, catchup=False, schedule_interval='0 3 * * *') as dag:

    ebi = get_json_secret('ebi_db_conn')['db_connections']['fi_dm_metrics']
    conn_id = 'ebi_datamart'
    pool_id = 'ebi_etl_pool'

    cfin_daily_flash = MsSqlOperator(
        sql='select 1 as n;',
        task_id='test__fi_dm_metrics__access',
        autocommit=True,
        mssql_conn_id=conn_id,
        pool=pool_id,
    )
