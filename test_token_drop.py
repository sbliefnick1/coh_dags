from datetime import datetime, timedelta

import pendulum
from airflow import DAG
from airflow.sensors.sql import SqlSensor
from airflow.operators.dummy import DummyOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 11, 28, tzinfo=pendulum.timezone('America/Los_Angeles')),
    'concurrency': 8,
    'email': ['jharris@coh.org'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

clarity_token_query = '''
    select
        case when max(DRP_TS) < getdate() then 1 else 0 end
    from FI_EDW_TOKEN.dbo.EDW_TOKEN_DROPS with(nolock)
    where SRC_NM = 'CLARITY'
'''

epsi_token_query = '''
    select
        case when max(DRP_TS) < getdate() then 1 else 0 end
    from FI_EDW_TOKEN.dbo.EDW_TOKEN_DROPS with(nolock)
    where SRC_NM = 'EPSI2DM'
'''

morrisey_token_query = '''
    select
        case when max(DRP_TS) < getdate() then 1 else 0 end
    from FI_EDW_TOKEN.dbo.EDW_TOKEN_DROPS with(nolock)
    where SRC_NM = 'MOR2DM'
'''

with DAG('test_token_drop', default_args=default_args, catchup=False, schedule_interval='5 0 * * *') as dag:
    
    # define sensor tasks
    clarity_fresh = SqlSensor(
        task_id='clarity_fresness',
        conn_id='qa_ebi_datamart',
        sql=clarity_token_query,
        pool='default_pool',
        poke_interval=300,
        mode='reschedule',
        dag=dag,
    )

    epsi_fresh = SqlSensor(
        task_id='epsi_fresness',
        conn_id='qa_ebi_datamart',
        sql=epsi_token_query,
        pool='default_pool',
        poke_interval=300,
        mode='reschedule',
        dag=dag,
    )

    morrisey_fresh = SqlSensor(
        task_id='morrisey_fresness',
        conn_id='qa_ebi_datamart',
        sql=morrisey_token_query,
        pool='default_pool',
        poke_interval=300,
        mode='reschedule',
        dag=dag,
    )


    # define dummy transform tasks
    clarity_trans= DummyOperator(
        task_id='clarity_only_transform',
        pool='default_pool',
        dag=dag,
    )

    clarity_epsi_trans = DummyOperator(
        task_id='clarity_plus_epsi_transform',
        pool='default_pool',
        dag=dag,
    )

    clarity_morr_trans = DummyOperator(
        task_id='clarity_plus_morrisey_transform',
        pool='default_pool',
        dag=dag,
    )


    # order of operations
    clarity_fresh >> clarity_trans
    clarity_fresh >> clarity_epsi_trans
    clarity_fresh >> clarity_morr_trans
    epsi_fresh >> clarity_epsi_trans
    morrisey_fresh >> clarity_morr_trans
