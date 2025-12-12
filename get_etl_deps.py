import logging
import re
from datetime import datetime, timedelta
from pathlib import Path
from urllib.parse import quote_plus

import pandas as pd
import pendulum
import sqlalchemy as sa
from airflow import DAG
from airflow.operators.python import PythonOperator
from auxiliary.outils import get_json_secret
from tableaudocumentapi import Datasource

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2018, 10, 2, tzinfo=pendulum.timezone('America/Los_Angeles')),
    'email': ['sbliefnick@coh.org', 'jharris@coh.org'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
    }

dag = DAG('get_etl_deps', default_args=default_args, catchup=False, schedule_interval='30 23 * * *')


ds_folder = Path('/var/nfsshare/datasources/')


def query_and_save(db_engine):
    # get procedure-view relationship
    sql = 'select * from vw_ebi_airflow_etl_diagram'


query_and_save_deps = PythonOperator(task_id='query_and_save_deps',
                                     python_callable=query_and_save,
                                     op_kwargs={'db_engine': ppw_engine},
                                     dag=dag)

query_and_save_deps
