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
    'retries': 45,
}

def token_query(src):
    token_query_sql = f'''
        select
            case
                when cast(max(DRP_TS) as date) = cast(getdate() as date)
                        and max(DRP_TS) < getdate() 
                    then 1
                else 0
            end
        from FI_EDW_TOKEN.dbo.EDW_TOKEN_DROPS with(nolock)
        where SRC_NM = '{src}'
    '''

    return token_query_sql


poke_int = 600

with DAG('test_token_drop', default_args=default_args, catchup=False, schedule_interval='5 2 * * *') as dag:

    # define sensor tasks
    clarity_fresh = SqlSensor(
        task_id='clarity_fresness',
        conn_id='qa_ebi_datamart',
        sql=token_query('CLARITY'),
        pool='default_pool',
        poke_interval=poke_int,
        mode='reschedule',
        dag=dag,
    )

    epsi_fresh = SqlSensor(
        task_id='epsi_fresness',
        conn_id='qa_ebi_datamart',
        sql=token_query('EPSI2DM'),
        pool='default_pool',
        poke_interval=poke_int,
        mode='reschedule',
        dag=dag,
    )

    morrisey_fresh = SqlSensor(
        task_id='morrisey_fresness',
        conn_id='qa_ebi_datamart',
        sql=token_query('MOR2DM'),
        pool='default_pool',
        poke_interval=poke_int,
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
