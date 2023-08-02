from datetime import datetime

import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator
from auxiliary.outils import get_json_secret
import tableauserverclient as TSC
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

    ebi = get_json_secret('ebi_db_conn')['db_connections']['fi_dm_ebi']
    auth = TSC.TableauAuth(ebi['user'].split(sep='\\')[1], ebi['password'])
    server = TSC.Server('https://ebi.coh.org', use_server_version=True)
    
    conn_id = 'ebi_datamart'
    pool_id = 'ebi_etl_pool'
    
    base_url = 'https://vpxrstudio.coh.org/content/f14d3e1b-b477-4660-ad17-c95b88a1bd09'
    token = Variable.get('metrics_api_token')

    def refresh_ds(tableau_server, tableau_authentication, ds_luid):
        with server.auth.sign_in(tableau_authentication):
            server.datasources.refresh(ds_luid)
    
    def refresh_node(node_url, api_token):
        resp = requests.put(
            node_url,
            headers={'x-access-token': api_token},
            verify=False,
        )
        print(f'Sending PUT request to {node_url}')
        print(resp.content)
        if resp.status_code != 200:
            raise ValueError(f'PUT operation failed with response')
        time.sleep(5)


    base_run = PythonOperator(
        task_id='run_base_metrics',
        python_callable=refresh_node,
        op_kwargs={
            'node_url': f'{base_url}/refresh/base/prod/all',
            'api_token': token
        },
        pool='metrics_pool',
    )

    instance_run = PythonOperator(
        task_id='run_instance_metrics',
        python_callable=refresh_node,
        op_kwargs={
            'node_url': f'{base_url}/refresh/instance/prod/all',
            'api_token': token
        },
        pool='metrics_pool',
    )

    collection_run = PythonOperator(
        task_id='run_collection_metrics',
        python_callable=refresh_node,
        op_kwargs={
            'node_url': f'{base_url}/refresh/collection/prod/all',
            'api_token': token
        },
        pool='metrics_pool',
    )

    qrrm_monthly = MsSqlOperator(
        sql='drop table FI_DM_QRRM.dbo.Enterprise_Metrics_Quality_Monthly_Scorecard; select * into FI_DM_QRRM.dbo.Enterprise_Metrics_Quality_Monthly_Scorecard from FI_DM_METRICS.collections.quality_monthly_scorecard;',
        task_id='qrrm_monthly_metrics_to_fi_dm_qrrm',
        autocommit=True,
        mssql_conn_id=conn_id,
        pool=pool_id,
    )

    oc_daily_extract = PythonOperator(
        task_id='refresh_oc_daily_financials_extract',
        python_callable=refresh_ds,
        op_kwargs={
            'tableau_server': server,
            'tableau_authentication': auth,
            'ds_luid': 'bfacbd49-df60-4dfa-aa4f-24006fb8952a'
        },
        priority_weight=100,
    )

    acces_ops_extract = PythonOperator(
        task_id='refresh_access_operations_scorecard_extract',
        python_callable=refresh_ds,
        op_kwargs={
            'tableau_server': server,
            'tableau_authentication': auth,
            'ds_luid': '97d0cf7d-eafb-4cca-a031-7b5c1d8ad799'
        },
        priority_weight=100,
    )

    cfin_daily_flash_extract = PythonOperator(
        task_id='refresh_cfin_daily_flash_extract',
        python_callable=refresh_ds,
        op_kwargs={
            'tableau_server': server,
            'tableau_authentication': auth,
            'ds_luid': 'fcfcba9e-023b-446f-929c-afc037c74b90'
        },
        priority_weight=100,
    )

    base_run >> instance_run >> collection_run

    collection_run >> oc_daily_extract
    collection_run >> acces_ops_extract
    collection_run >> cfin_daily_flash_extract
    collection_run >> qrrm_monthly
