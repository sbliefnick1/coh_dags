from datetime import datetime, timedelta

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from auxiliary.outils import get_json_secret
import tableauserverclient as TSC

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2018, 9, 12, tzinfo=pendulum.timezone('America/Los_Angeles')),
    'concurrency': 6,
    'email': ['jharris@coh.org', 'sbliefnick@coh.org'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
    }

dag = DAG('refresh_dbt_extracts', default_args=default_args, catchup=False, schedule_interval='40 5 * * *')

def refresh_extracts():
    server_url = 'https://ebi.coh.org'
    ebi = get_json_secret('ebi_db_conn')['db_connections']['fi_dm_ebi']
    tableau_auth = TSC.TableauAuth(ebi['user'].split(sep='\\')[1], ebi['password'])
    server = TSC.Server(server_url, use_server_version=True)

    query = '''{
    tags (filter: {name: "dbt"}){
        publishedDatasources {
        luid
        }
    }
    }'''

    with server.auth.sign_in(tableau_auth):
        ds_ls = server.metadata.query(query)['data']['tags'][0]['publishedDatasources']
        [server.datasources.get_by_id(ds['luid']).refresh() for ds in ds_ls]

r = PythonOperator(
        task_id='refresh_dbt_extracts',
        python_callable=refresh_extracts,
        dag=dag
        )

r
