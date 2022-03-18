from datetime import datetime, timedelta

import pendulum
from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.operators.python import PythonOperator
from auxiliary.outils import get_json_secret
import tableauserverclient as TSC

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

dag = DAG('refresh_metrics', default_args=default_args, catchup=False, schedule_interval='35 5 * * *')

ebi = get_json_secret('ebi_db_conn')['db_connections']['fi_dm_ebi']
auth = TSC.TableauAuth(ebi['user'].split(sep='\\')[1], ebi['password'])
server = TSC.Server('https://ebi.coh.org', use_server_version=True)

git_pull_latest = 'cd C:\\Anaconda\\ETL\\metrics && git pull'
refresh_oc_daily_financials_table = 'cd C:\\Anaconda\\ETL\\metrics\\collections && conda activate metrics && python oc_daily_financial_statistics.py'
refresh_cfin_daily_flash_table = 'cd C:\\Anaconda\\ETL\\metrics\\collections && conda activate metrics && python clinical_finance_daily_flash.py'
refresh_metrics_metadata_table = 'cd C:\\Anaconda\\ETL\\metrics\\dictionary && conda activate metrics && python load_dictionary_data.py'

def refresh_ds(tableau_server, tableau_authentication, ds_luid):
    with server.auth.sign_in(tableau_authentication):
        server.datasources.refresh(ds_luid)

gp = SSHOperator(ssh_conn_id='tableau_server',
                task_id='git_pull_latest',
                command=git_pull_latest,
                dag=dag)

rocdt = SSHOperator(ssh_conn_id='tableau_server',
                task_id='refresh_oc_daily_financials_table',
                command=refresh_oc_daily_financials_table,
                dag=dag)

rcfdft = SSHOperator(ssh_conn_id='tableau_server',
                task_id='refresh_cfin_daily_flash_table',
                command=refresh_cfin_daily_flash_table,
                dag=dag)

rmmt = SSHOperator(ssh_conn_id='tableau_server',
                task_id='refresh_metrics_metadata_table',
                command=refresh_metrics_metadata_table,
                dag=dag)

rocde = PythonOperator(
        task_id='refresh_oc_daily_financials_extract',
        python_callable=refresh_ds,
        op_kwargs={'tableau_server': server, 'tableau_authentication': auth, 'ds_luid': 'bfacbd49-df60-4dfa-aa4f-24006fb8952a'},
        dag=dag
        )

rcfdfe = PythonOperator(
        task_id='refresh_cfin_daily_flash_extract',
        python_callable=refresh_ds,
        op_kwargs={'tableau_server': server, 'tableau_authentication': auth, 'ds_luid': 'fcfcba9e-023b-446f-929c-afc037c74b90'},
        dag=dag
        )

rmme = PythonOperator(
        task_id='refresh_metrics_metadata_extract',
        python_callable=refresh_ds,
        op_kwargs={'tableau_server': server, 'tableau_authentication': auth, 'ds_luid': 'e64852e7-d0cf-43ec-b3d1-55f6bdda421e'},
        dag=dag
        )

gp >> rocdt >> rocde
gp >> rmmt >> rmme
gp >> rcfdft >> rcfdfe
