from datetime import datetime, timedelta

import pendulum
from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 2, 28, tzinfo=pendulum.timezone('America/Los_Angeles')),
    'email': ['jharris@coh.org'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

with DAG('orchestrate_ebi_cloud', default_args=default_args, catchup=False, schedule_interval='0 4 * * *') as dag:

    repo = 'C:\\Users\\ebitabuser\\Documents\\ebi-cloud-orchestration'
    enviro = 'dbt_automations'

    wait_for_sources_bash = f'cd {repo} && conda activate {enviro} && python wait_for_sources.py'
    wait_for_ebi_sources_bash = f'cd {repo} && conda activate {enviro} && python wait_for_ebi_sources.py'
    ae_dbt_build_bash = f'cd {repo} && conda activate {enviro} && python ae_dbt_build.py'
    ae_dbt_test_bash = f'cd {repo} && conda activate {enviro} && python ae_dbt_test.py'
    ebi_dbt_build_bash = f'cd {repo} && conda activate {enviro} && python ebi_dbt_build.py'
    cfin_dbt_build_bash = f'cd {repo} && conda activate {enviro} && python cfin_dbt_build.py'
    refresh_tableau_extracts_bash = f'cd {repo} && conda activate {enviro} && python refresh_tableau_extracts.py'
    
    
    wait_for_sources = SSHOperator(
        ssh_conn_id='ebi_etl_server',
        task_id='wait_for_sources',
        command=wait_for_sources_bash,
    )

    wait_for_ebi_sources = SSHOperator(
        ssh_conn_id='ebi_etl_server',
        task_id='wait_for_ebi_sources',
        command=wait_for_ebi_sources_bash,
    )

    ae_dbt_build = SSHOperator(
        ssh_conn_id='ebi_etl_server',
        task_id='ae_dbt_build',
        command=ae_dbt_build_bash,
    )

    ae_dbt_test = SSHOperator(
        ssh_conn_id='ebi_etl_server',
        task_id='ae_dbt_test',
        command=ae_dbt_test_bash,
    )

    ebi_dbt_build = SSHOperator(
        ssh_conn_id='ebi_etl_server',
        task_id='ebi_dbt_build',
        command=ebi_dbt_build_bash,
    )

    cfin_dbt_build = SSHOperator(
        ssh_conn_id='ebi_etl_server',
        task_id='cfin_dbt_build',
        command=cfin_dbt_build_bash,
    )

    refresh_tableau_extracts = SSHOperator(
        ssh_conn_id='ebi_etl_server',
        task_id='refresh_tableau_extracts',
        command=refresh_tableau_extracts_bash,
    )

    wait_for_sources >> ae_dbt_build
    ae_dbt_build >> ebi_dbt_build
    ae_dbt_build >> ae_dbt_test
    wait_for_ebi_sources >> ebi_dbt_build
    ebi_dbt_build >> refresh_tableau_extracts
    ae_dbt_build >> cfin_dbt_build
