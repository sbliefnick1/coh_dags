from datetime import datetime, timedelta
from time import sleep
from urllib.parse import quote_plus

import pendulum
import sqlalchemy as sa
from airflow import DAG
from airflow.operators.python import PythonOperator
from auxiliary.outils import get_json_secret
from sqlalchemy.exc import ProgrammingError


def attempt_connection(db_engine):
    while True:
        try:
            db_engine.connect()
            break
        except ProgrammingError as e:
            sleep(300)


ebi = get_json_secret('ebi_db_conn')['db_connections']['fi_dm_ebi']

params = quote_plus('DRIVER={driver};'
                    'SERVER={server};'
                    'DATABASE={database};'
                    'UID={user};'
                    'PWD={password};'
                    'PORT={port};'
                    'TDS_Version={tds_version};'
                    .format(**ebi))

engine = sa.create_engine('mssql+pyodbc:///?odbc_connect={}'.format(params))

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2018, 9, 1, tzinfo=pendulum.timezone('America/Los_Angeles')),
    'email': ['sbliefnick@coh.org'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)}

dag = DAG('probe_db_access', default_args=default_args, schedule_interval='30 4 * * *', catchup=False)

t1 = PythonOperator(task_id='attempt_to_connect',
                    python_callable=attempt_connection,
                    op_kwargs={'db_engine': engine},
                    dag=dag)
