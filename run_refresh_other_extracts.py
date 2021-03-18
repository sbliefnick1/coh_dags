from datetime import datetime, timedelta

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
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

dag = DAG('run_refresh_other_extracts', default_args=default_args, catchup=False, schedule_interval='40 5 * * *')

# deps = ExternalTaskSensor(
#        external_dag_id='run_daily_census',
#        external_task_id='refresh_daily_census',
#        task_id='wait_for_daily_census',
#        dag=dag
#        )

telehlth_bash = 'cd C:\\Anaconda\\ETL\\tableau && python TableauOutofStateApptProvs.py'

datasources = [
    {'task_id': 'refresh_clinical_trials_catchment',
     'datasource_id': 'ddd7ff23-0e45-4549-ac4e-f9cad97c3b31'},
    {'task_id': 'refresh_psft_hr_ps_coh_web_intrfac',
     'datasource_id': 'c8744e87-cb5c-4adc-b96d-19ed443879bc'},
    {'task_id': 'refresh_availability',
     'datasource_id': '85460963-7e81-4e5d-aaff-1a044cdc6c80'},
    {'task_id': 'refresh_pb_denials',
     'datasource_id': '66174e43-9ae5-43a8-b785-1080cf651b10'},
    {'task_id': 'refresh_direct_cost_medication',
     'datasource_id': '9733efff-5ea1-4f73-8062-fb00b9c30ced'},
    {'task_id': 'refresh_direct_cost_supplies_implants',
     'datasource_id': 'c417926a-4646-48ed-b889-6a89fe590f64'},
    {'task_id': 'refresh_arhb_payor_metrics',
     'datasource_id': '46da3492-9204-401e-b46f-a31ba2aaf75d'},
    {'task_id': 'refresh_corporate_accounting_accounts_payable',
     'datasource_id': 'c30f1707-ad4a-449c-8150-111575700865'},
    {'task_id': 'refresh_daily_its_calls_and_tickets',
     'datasource_id': '9b0f200d-4eb6-4c8c-a581-0c0383b6d1ff'},
    {'task_id': 'refresh_capital_project_security_check',
     'datasource_id': '23d5115f-eadc-4366-95b7-2a82aa710056'},
    {'task_id': 'refresh_revenue_cycle_pb_workqueue_telehealth',
     'datasource_id': '3fd8f8f1-439c-499c-9649-e15a3251bd35'},
    {'task_id': 'refresh_cash_forecast',
     'datasource_id': '8162e40a-b2d6-4930-b147-018b44d63897'}
    ]

for d in datasources:
    task = PythonOperator(
            task_id=d['task_id'],
            python_callable=refresh_tableau_extract,
            op_kwargs={'datasource_id': d['datasource_id']},
            dag=dag
            )

    task

sync = SSHOperator(ssh_conn_id='tableau_server',
                   task_id='Sync_Telehealth_Providers',
                   command=telehlth_bash,
                   dag=dag)

sync
