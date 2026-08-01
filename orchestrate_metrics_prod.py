from datetime import datetime

import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator
from airflow.sensors.sql import SqlSensor
from auxiliary.outils import get_json_secret
import requests
import time


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2018, 9, 12, tzinfo=pendulum.timezone('America/Los_Angeles')),
    'email': ['jharris@coh.org', 'nbyers@coh.org'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
}

with DAG('orchestrate_metrics_prod', default_args=default_args, catchup=False, schedule_interval='0 4 * * *') as dag:
    
    conn_id = 'ebi_datamart'
    pool_id = 'ebi_etl_pool'
    
    repo = r'C:\Users\ebitabuser\Documents\metrics-cli'
    enviro = 'metrics_cli'
    python_exe = rf'C:\Users\ebitabuser\AppData\Local\Miniconda3\envs\{enviro}\python.exe'
    prefix = f'cd {repo} && "{python_exe}"'

    def check_date(dbt_date):
    # check that max dbt run was today
        return datetime.today().strftime('%Y-%m-%d') == dbt_date.strftime('%Y-%m-%d')

    def run_metric_type(type):
    # run same command for each metrics type with different type parameter
        return f'{prefix} refresh_metrics.py --type {type} --environment prod'


    check_dbt = SqlSensor(
        task_id='check_dbt_done',
        conn_id='ebi_datamart',
        sql='select cast(max(_dbt_update) as date) from edw_bdm.data_quality.dbt_results_log with(nolock)',
        success=check_date,
    )
    
    base_run = SSHOperator(
        ssh_conn_id='ebi_etl_server',
        task_id='run_base_metrics',
        command=run_metric_type('base'),
    )

    instance_run = SSHOperator(
        ssh_conn_id='ebi_etl_server',
        task_id='run_instance_metrics',
        command=run_metric_type('instance'),
    )

    collection_run = SSHOperator(
        ssh_conn_id='ebi_etl_server',
        task_id='run_collection_metrics',
        command=run_metric_type('collection'),
    )

    qrrm_monthly = MsSqlOperator(
        sql='drop table FI_DM_QRRM.dbo.Enterprise_Metrics_Quality_Monthly_Scorecard; select * into FI_DM_QRRM.dbo.Enterprise_Metrics_Quality_Monthly_Scorecard from FI_DM_METRICS.collections.quality_monthly_scorecard;',
        task_id='qrrm_monthly_metrics_to_fi_dm_qrrm',
        autocommit=True,
        mssql_conn_id=conn_id,
        pool=pool_id,
    )

    
    check_dbt >> base_run >> instance_run >> collection_run >> qrrm_monthly
