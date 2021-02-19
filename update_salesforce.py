from datetime import timedelta, datetime

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from auxiliary.outils import refresh_tableau_extract

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['sbliefnick@coh.org', 'jharris@coh.org'],
    'email_on_failure': True,
    'email_on_retry': False,
    'start_date': datetime(2019, 3, 6, tzinfo=pendulum.timezone('America/Los_Angeles')),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    }

dag = DAG('update_salesforce', default_args=default_args, catchup=False, schedule_interval='0 8-20 * * *')

t1_bash = 'cd C:\\Anaconda\\ETL\\salesforce && python get_salesforce.py'

t1 = SSHOperator(ssh_conn_id='tableau_server',
                 task_id='run_get_salesforce',
                 command=t1_bash,
                 dag=dag)

datasources = [
    #{'task_id': 'refresh_remedy_force_incident_trend',
    # 'datasource_id': 'B5C928D5-D60B-4ECA-A3F5-AF14078A8629'},
    {'task_id': 'refresh_salesforce_new_patient_leakage',
     'datasource_id': '5B768E79-F89A-4B8F-8F83-2D87A641DC1D'},
    {'task_id': 'refresh_remedy_force_incident',
     'datasource_id': '3F3A843B-CEE9-48B3-A045-658233E1437F'},
    {'task_id': 'refresh_provider_loa_submission',
     'datasource_id': '1f573034-ea9c-4ce2-9d1f-aeeda8e90c65'},
    {'task_id': 'refresh_ehs_risk_assessment',
     'datasource_id': '7dd7721b-908b-4e69-b701-225ca78a1e8c'},
    ]

for d in datasources:
    task = PythonOperator(
            task_id=d['task_id'],
            python_callable=refresh_tableau_extract,
            op_kwargs={'datasource_id': d['datasource_id']},
            dag=dag
            )

    t1 >> task
