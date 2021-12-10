from datetime import datetime, timedelta

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.sql import SqlSensor
from auxiliary.outils import refresh_tableau_extract


def check_date(d):
    # check that max call date is greater than or equal to yesterday
    return (datetime.today() + timedelta(days=-1)).strftime('%Y-%m-%d') <= d.strftime('%Y-%m-%d')


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

dag = DAG('run_call_center', default_args=default_args, catchup=False, schedule_interval='40 5 * * *')

# deps = ExternalTaskSensor(
#        external_dag_id='run_daily_census',
#        external_task_id='refresh_daily_census',
#        task_id='wait_for_daily_census',
#        dag=dag
#        )

call_date_sql = """
select MAX(CAST(rqi.[DateTime] as date)) as max_cisco_calldate

from ccehds_t_router_queue_interval rqi with (nolock)

where CAST(rqi.[DateTime] as date) < CAST(GETDATE() as date)
  and rqi.precisionqueueid != 5082

having SUM(ISNULL(rqi.callsoffered, 0)) > 0
"""

check_max_call_date = SqlSensor(
        task_id='check_max_call_date',
        conn_id='ebi_datamart',
        sql=call_date_sql,
        success=check_date,
        dag=dag,
        )

ACC = PythonOperator(
        task_id='refresh_avaya_call_center',
        python_callable=refresh_tableau_extract,
        op_kwargs={'datasource_id': '6EE81264-DFE1-4318-8397-9EC853DF7085'},
        dag=dag
        )

ACCA = PythonOperator(
        task_id='refresh_avaya_call_center_agents',
        python_callable=refresh_tableau_extract,
        op_kwargs={'datasource_id': '082E7473-5803-4681-A847-3ADAB4B1E8E7'},
        dag=dag
        )

AAGP = PythonOperator(
        task_id='refresh_avaya_agent_group_planner',
        python_callable=refresh_tableau_extract,
        op_kwargs={'datasource_id': 'B4F5407D-36E1-473D-87F1-D5B6AE0A9527'},
        dag=dag
        )

AAGPH = PythonOperator(
        task_id='refresh_avaya_agent_group_planner_hour',
        python_callable=refresh_tableau_extract,
        op_kwargs={'datasource_id': '8B2C21BF-9EFC-491A-BC55-855F1B065393'},
        dag=dag
        )

# AVV = PythonOperator(
#         task_id='refresh_avaya_vdn_vector',
#         python_callable=refresh_tableau_extract,
#         op_kwargs={'datasource_id': '7f9bdba7-b64e-4225-a777-490dc14ffb69'},
#         dag=dag
#         )

ACCAT = PythonOperator(
        task_id='refresh_avaya_call_center_agents_trace',
        python_callable=refresh_tableau_extract,
        op_kwargs={'datasource_id': 'd79bb359-8404-4db1-867e-8ca5ac9069d2'},
        dag=dag
        )

SURV = PythonOperator(
        task_id='refresh_cisco_call_center_surveys',
        python_callable=refresh_tableau_extract,
        op_kwargs={'datasource_id': '1e51f282-6346-473f-b4ca-2fa2bff18d6f'},
        dag=dag
        )

# deps >> ACC
# deps >> ACCA
# deps >> AAGP
# deps >> AAGPH
# deps >> AVV

check_max_call_date >> ACC
ACCA
AAGP
AAGPH
# AVV
ACCAT
SURV
