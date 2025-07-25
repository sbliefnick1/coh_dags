from datetime import datetime, timedelta

import pendulum
from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2018, 9, 12, tzinfo=pendulum.timezone('America/Los_Angeles')),
    'email': ['jharris@coh.org'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2)
}

with DAG('refresh_core_extracts', default_args=default_args, catchup=False, concurrency=4, schedule_interval='0 16 * * *') as dag:

    repo = r'C:\Users\ebitabuser\Documents\ebi-automations'
    enviro = 'dbt_automations'
    python_exe = rf'C:\Users\ebitabuser\AppData\Local\Miniconda3\envs\{enviro}\python.exe'
    prefix = f'cd {repo} && "{python_exe}"'

    run_iip_extracts = SSHOperator(
        ssh_conn_id='ebi_etl_server',
        task_id='run_iip_extracts',
        command=f'{prefix} refresh_iip_time_to_seen_metrics.py',
    )

    run_dbt_common_coverage_refresh = SSHOperator(
        ssh_conn_id='ebi_etl_server',
        task_id='run_dbt_common_coverage_refresh',
        command=f'{prefix} get_coverage_stats.py',
    )

    run_ebi_dbt_housekeeping = SSHOperator(
        ssh_conn_id='ebi_etl_server',
        task_id='run_dbt_housekeeping_refresh',
        command=f'{prefix} ebi_dbt_housekeeping.py',
    )

    run_ebi_snowflake_progress = SSHOperator(
        ssh_conn_id='ebi_etl_server',
        task_id='run_ebi_snowflake_progress_refresh',
        command=f'{prefix} ebi_snowflake_progress.py',
    )
    
    run_iip_extracts
    run_dbt_common_coverage_refresh
    run_ebi_dbt_housekeeping
    run_ebi_snowflake_progress
