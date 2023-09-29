from datetime import datetime, timedelta

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from auxiliary.outils import refresh_tableau_extract

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2018, 9, 12, tzinfo=pendulum.timezone('America/Los_Angeles')),
    'email': ['jharris@coh.org'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
    }

with DAG('refresh_total_charges', default_args=default_args, catchup=False, schedule_interval='0 17 * * *') as dag:

    rvu = PythonOperator(
        task_id='refresh_rvu_extract',
        python_callable=refresh_tableau_extract,
        op_kwargs={'datasource_id': 'c08148a1-cf27-48df-8c8f-fc29f2c77c12'},
    )

    tc = PythonOperator(
        task_id='refresh_total_charges',
        python_callable=refresh_tableau_extract,
        op_kwargs={'datasource_id': 'ad80c2d3-3926-4516-af11-55bac64735ac'},
    )

    new = PythonOperator(
        task_id='refresh_new_em_provider',
        python_callable=refresh_tableau_extract,
        op_kwargs={'datasource_id': 'ea9a44b2-3f27-4560-9a00-4e056ede95bd'},
    )

    trj = PythonOperator(
        task_id='refresh_patient_trajectory',
        python_callable=refresh_tableau_extract,
        op_kwargs={'datasource_id': '951a76e5-e7a7-466a-a0aa-6b592bd9a370'},
    )

    cim = PythonOperator(
        task_id='refresh_cim_charges',
        python_callable=refresh_tableau_extract,
        op_kwargs={'datasource_id': '6e39fb3d-fdc1-41ee-8a2a-3f819a12fea5'},
    )

    rvu
    tc >> new >> trj
    tc >> cim
