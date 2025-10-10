from datetime import datetime, timedelta

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from auxiliary.outils import refresh_tableau_extract

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

dag = DAG('run_refresh_other_extracts', default_args=default_args, catchup=False, schedule_interval='0 10 * * *')

datasources = [
    {'task_id': 'refresh_clinical_trials_catchment',
     'datasource_id': 'ddd7ff23-0e45-4549-ac4e-f9cad97c3b31'},
    {'task_id': 'refresh_psft_hr_ps_coh_web_intrfac',
     'datasource_id': 'c8744e87-cb5c-4adc-b96d-19ed443879bc'},
    {'task_id': 'refresh_pb_denials',
     'datasource_id': '66174e43-9ae5-43a8-b785-1080cf651b10'},
    {'task_id': 'refresh_arhb_payor_metrics',
     'datasource_id': '46da3492-9204-401e-b46f-a31ba2aaf75d'},
    {'task_id': 'refresh_corporate_accounting_accounts_payable',
     'datasource_id': 'c30f1707-ad4a-449c-8150-111575700865'},
    {'task_id': 'refresh_capital_project_security_check',
     'datasource_id': '23d5115f-eadc-4366-95b7-2a82aa710056'},
    {'task_id': 'refresh_pb_tdl_transactions',
     'datasource_id': '8a83879d-6937-4a31-a784-21107733854f'},
    {'task_id': 'refresh_corporate_accounting_ap_aging',
     'datasource_id': '04b12b4d-fb90-483e-b0ad-653f4e85867a'},
    {'task_id': 'refresh_daily_tray_delivery',
     'datasource_id': '009749f0-02e0-443a-ad3d-b6b551192231'},
    {'task_id': 'refresh_cancer_center_publications',
     'datasource_id': 'a81be1ce-498c-4a79-8730-5fdcc20a1cec'},
    {'task_id': 'refresh_340B_dsh_patient_days',
     'datasource_id': '407119d8-7720-4304-9d97-776fcb65c987'},
    {'task_id': 'refresh_spec_pharmacy_credit_card',
     'datasource_id': '62f4c05c-355c-472a-a98f-df48dfc3fdbf'},
    {'task_id': 'refresh_ccsg_publications',
     'datasource_id': 'e8216b8e-02b4-4681-b367-c4a49170903f'}
    ]

for d in datasources:
    task = PythonOperator(
            task_id=d['task_id'],
            python_callable=refresh_tableau_extract,
            op_kwargs={'datasource_id': d['datasource_id']},
            dag=dag
            )

task
