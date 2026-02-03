from datetime import datetime, timedelta

import pendulum
from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator

from refresh_metrics import git_pull_latest

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

with DAG('refresh_ebi_snowflake_data', default_args=default_args, concurrency=1, catchup=False, schedule_interval='0 20 * * *') as dag:

    repo = r'C:\Users\ebitabuser\Documents\ebi-etl'
    enviro = 'ebi_data_engineering'
    python_exe = rf'C:\Users\ebitabuser\AppData\Local\Miniconda3\envs\{enviro}\python.exe'
    prefix = f'cd {repo} && "{python_exe} -m ebi_etl --tasks"'

    git = SSHOperator(
        ssh_conn_id='ebi_etl_server',
        task_id='git_pull_latest',
        command=f'cd {repo} && git pull',
    )

    rvu = SSHOperator(
        ssh_conn_id='ebi_etl_server',
        task_id='load_rvus',
        command=f'{prefix} rvus',
    )

    tab_vws = SSHOperator(
        ssh_conn_id='ebi_etl_server',
        task_id='load_tableau_metadata_views',
        command=f'{prefix} tableau_metadata_views',
    )

    tab_wbs = SSHOperator(
        ssh_conn_id='ebi_etl_server',
        task_id='load_tableau_metadata_workbooks',
        command=f'{prefix} tableau_metadata_workbooks',
    )

    tab_ds = SSHOperator(
        ssh_conn_id='ebi_etl_server',
        task_id='load_tableau_metadata_published_data_sources',
        command=f'{prefix} tableau_metadata_published_data_sources',
    )

    tab_adm_evnts = SSHOperator(
        ssh_conn_id='ebi_etl_server',
        task_id='load_tableau_admin_insights_ts_events',
        command=f'{prefix} tableau_admin_insights_ts_events',
    )

    tab_adm_perms = SSHOperator(
        ssh_conn_id='ebi_etl_server',
        task_id='load_tableau_admin_insights_permissions',
        command=f'{prefix} tableau_admin_insights_permissions',
    )

    tab_adm_sc = SSHOperator(
        ssh_conn_id='ebi_etl_server',
        task_id='load_tableau_admin_insights_site_content',
        command=f'{prefix} tableau_admin_insights_site_content',
    )

    git >> rvu
    git >> tab_vws
    git >> tab_wbs
    git >> tab_ds
    git >> tab_adm_evnts
    git >> tab_adm_perms
    git >> tab_adm_sc
