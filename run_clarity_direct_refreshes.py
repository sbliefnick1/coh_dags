from datetime import datetime, timedelta

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.sql import SqlSensor
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

dag = DAG('run_clarity_direct_refreshes', default_args=default_args, catchup=False, schedule_interval='30 0 * * *')

sql_query = '''
select case when max(exec_end_time) > cast(getdate() as date) then 1 else 0 end
from cr_stat_execution
where exec_name = 'ETL'
  and status in ('Warning', 'Success')
'''

query = SqlSensor(
        task_id='query_etl_status',
        conn_id='EPCCLAPRDAGL',
        sql=sql_query,
        dag=dag
        )

hbs = PythonOperator(
        task_id='refresh_hospital_billing_summary',
        python_callable=refresh_tableau_extract,
        op_kwargs={'datasource_id': '97774949-8372-4128-9730-3C9432C0305E'},
        dag=dag
        )

dcc = PythonOperator(
        task_id='refresh_daily_census_clarity',
        python_callable=refresh_tableau_extract,
        op_kwargs={'datasource_id': '0ED0A5E0-30FC-4A9C-8A6A-C906CEAA1F3E'},
        dag=dag
        )

dcnp = PythonOperator(
        task_id='refresh_daily_census_nonip_patients',
        python_callable=refresh_tableau_extract,
        op_kwargs={'datasource_id': 'e4f386ac-abef-4ade-8a87-36560273e276'},
        dag=dag
        )

dcpdap = PythonOperator(
        task_id='refresh_daily_census_pending_direct_admit_patients',
        python_callable=refresh_tableau_extract,
        op_kwargs={'datasource_id': '16b2821f-0a51-4f3a-a032-50d034250531'},
        dag=dag
        )

pbs = PythonOperator(
        task_id='refresh_professional_billing_summary',
        python_callable=refresh_tableau_extract,
        op_kwargs={'datasource_id': 'df657eee-298a-4c1a-8fa9-cf7c5324699b'},
        dag=dag
        )

imt = PythonOperator(
        task_id='refresh_ib_messages_trend',
        python_callable=refresh_tableau_extract,
        op_kwargs={'datasource_id': '6f80eda8-803e-4cac-bf0a-162a4f2e86d1'},
        dag=dag
        )

query >> hbs
query >> dcc
query >> dcnp
query >> dcpdap
query >> pbs
query >> imt
