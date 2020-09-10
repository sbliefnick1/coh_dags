from datetime import datetime, timedelta
import pendulum

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable

import requests
import json

from auxiliary.outils import get_json_secret

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 9, 9, tzinfo=pendulum.timezone('America/Los_Angeles')),
    'email': ['jharris@coh.org'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
    }

dag = DAG('run_tableau_server_backup', default_args=default_args, catchup=False, schedule_interval=None)

ebi = get_json_secret('ebi_db_conn')['db_connections']['fi_dm_ebi']

server_url = Variable.get('tableau_server_url')


def backup():
    login_url = server_url + ":8850/api/0.5/login"
    logout_url = server_url + ":8850/api/0.5/logout"
    backup_endpoint = server_url + ":8850/api/0.5/backupFixedFile"

    username = ebi['user'].split(sep='\\')[1]
    password = ebi['password']

    headers = {
        'content-type': 'application/json'
        }

    auth = {
        'authentication': {
            'name': username,
            'password': password
            }
        }

    session = requests.Session()
    body = json.dumps(auth)
    now = datetime.now()
    now = now.strftime("%Y%m%dT%H%M%S")
    writep = "backup-" + now + ".tsbak"
    backup_url = backup_endpoint + "?writePath=" + writep + "&jobTimeoutSeconds=7200"

    login_resp = session.post(login_url, data=body, headers=headers, verify=False)
    backup_resp = session.post(backup_url)
    logout_resp = session.post(logout_url)


bs = PythonOperator(
        task_id='backup_tableau_server',
        python_callable=backup,
        dag=dag
        )