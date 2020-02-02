import logging
import os
from pathlib import Path
from datetime import date, datetime, timedelta
from urllib.parse import quote_plus

import pandas as pd
import pendulum
import sqlalchemy as sa

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.sftp_operator import SFTPOperator

from auxiliary.outils import get_json_secret

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 1, 10, tzinfo=pendulum.timezone('America/Los_Angeles')),
    'email': ['sbliefnick@coh.org', 'jharris@coh.org', 'ddeaville@coh.org'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=3)
    }

# runs on the 10th to make sure we have all of the previous month's data
dag = DAG('upload_narrativedx', default_args=default_args, catchup=False, schedule_interval='0 7 10 * *')

services = ['AS', 'IN', 'ON', 'MD']
basepath = Path('/var/nfsshare/files/narrativedx/')


def delete_older_file(service):
    path = basepath.joinpath(f'NarrativeDX - {service} - {{ ds }}.csv')

    try:
        os.remove(path)
    except FileNotFoundError:
        logging.info(FileNotFoundError)


def query_narrativedx(service):
    ebi = get_json_secret('ebi_db_conn')['db_connections']['fi_dm_ebi']

    ppw_params = quote_plus('DRIVER={driver};'
                            'SERVER={server};'
                            'DATABASE={database};'
                            'UID={user};'
                            'PWD={password};'
                            'PORT={port};'
                            'TDS_Version={tds_version};'
                            .format(**ebi))

    ppw_engine = sa.create_engine(f'mssql+pyodbc:///?odbc_connect={ppw_params}')

    with open(basepath.joinpath('narrativedx_query.sql')) as file:
        sql = file.read()

    # get custom dates if they exist in Airflow variables, otherwise do first and last day of prev month
    first_of_month = date.today().replace(day=1)
    end_date = Variable.get('narrativedx_end_date', default=first_of_month - timedelta(days=1))
    start_date = Variable.get('narrativedx_start_date', default=first_of_month - timedelta(days=end_date.day))

    sql = sql.format(start_date=start_date, end_date=end_date, surv=service)
    df = pd.read_sql(sql, ppw_engine)

    df.to_csv(basepath.joinpath(f'NarrativeDX - {service} - {{ next_ds }}.csv'))


queries = []
next_ds = "{{ next_ds }}"
for service in services:
    delete = PythonOperator(task_id=f'delete_older_{service}_file',
                            python_callable=delete_older_file,
                            op_kwargs={'service': service},
                            provide_context=True,
                            dag=dag)

    query = PythonOperator(task_id=f'query_narrativedx_{service}',
                           python_callable=query_narrativedx,
                           op_kwargs={'service': service},
                           provide_context=True,
                           dag=dag)

    sftp = SFTPOperator(task_id=f'upload_{service}_to_sftp',
                        ssh_conn_id='coh_sftp',
                        local_filepath=basepath.joinpath(f'NarrativeDX - {service} - {next_ds}.csv'),
                        remote_filepath=Path(f'/NarrativeDX - {service} - {next_ds}.csv'),
                        operation='put',
                        create_intermediate_dirs=True,
                        dag=dag)

    # send to archive if necessary

    # set each query downstream from the previous one in order not to slam the db
    if len(queries) > 0:
        queries[-1] >> query

    queries.append(query)

    delete >> query >> sftp
