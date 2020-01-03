from datetime import datetime, timedelta

import pandas as pd
import pendulum
import sqlalchemy as sa
import tableauserverclient as TSC

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator

from auxiliary.outils import get_json_secret

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2018, 12, 1, tzinfo=pendulum.timezone('America/Los_Angeles')),
    'email': ['sbliefnick@coh.org'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
    }

dag = DAG('kill_tableau_job_by_id', default_args=default_args, catchup=False, schedule_interval=None)


def get_job_id(tb_job_id):
    # TODO allow for a list to be input, but how is the variable returned from the UI if it's a list?
    pg = get_json_secret('ebi_db_conn')['db_connections']['tableau_pg']
    pg_params = '{user}:{password}@{server}:{port}/{database}'.format(**pg)
    tpg_engine = sa.create_engine('postgresql+psycopg2://{}'.format(pg_params))

    sql = 'select id, luid from background_jobs where id = {}'.format(tb_job_id)

    df = pd.read_sql(sql, tpg_engine)

    return str(df.luid[0])


def kill_job(**context):
    tb_job_id = context['task_instance'].xcom_pull(task_ids='get_job_id')
    ebi = get_json_secret('ebi_db_conn')['db_connections']['fi_dm_ebi']
    auth = TSC.TableauAuth(ebi['user'].split(sep='\\')[1], ebi['password'])
    server = TSC.Server(Variable.get('tableau_server_url'), use_server_version=True)

    server.auth.sign_in(auth)
    resp = server.jobs.cancel(tb_job_id)
    server.auth.sign_out()


get = PythonOperator(task_id='get_job_id',
                     python_callable=get_job_id,
                     op_kwargs={'tb_job_id': Variable.get('tableau_job_id')},
                     dag=dag)

kill = PythonOperator(task_id='kill_job',
                      python_callable=kill_job,
                      provide_context=True,
                      dag=dag)

get >> kill
