from datetime import datetime, timedelta

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.sql import SqlSensor
from auxiliary.outils import refresh_tableau_extract


def check_date(d):
    # check that max census date is yesterday
    return (datetime.today() + timedelta(days=-1)).strftime('%Y-%m-%d') == d


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2018, 9, 12, tzinfo=pendulum.timezone('America/Los_Angeles')),
    'email': ['jharris@coh.org', 'ddeaville@coh.org'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
    }

dag = DAG('run_daily_census', default_args=default_args, catchup=False, schedule_interval='35 5 * * *')

# deps = ExternalTaskSensor(
#        external_dag_id='get_etl_deps',
#        external_task_id='query_and_save_deps',
#        task_id='wait_for_dependencies_file',
#        dag=dag
#        )

check_max_census_date = SqlSensor(
        task_id='check_max_census_date',
        conn_id='ebi_datamart',
        sql='select MAX(EFFECTIVE_TIME) from CLARITY_CLARITY_ADT where EVENT_TYPE_C = 6',
        success=check_date,
        dag=dag,
        )

RDC = PythonOperator(
        task_id='refresh_daily_census',
        python_callable=refresh_tableau_extract,
        op_kwargs={'datasource_id': '86a56fc4-d842-40ad-8898-2249e302c88d'},
        dag=dag
        )

# deps >>
check_max_census_date >> RDC
