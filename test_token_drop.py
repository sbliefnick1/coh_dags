from datetime import datetime
from itertools import combinations

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
    'retries': 5,
    }

srcs = ['psfthr',
        'mor2dm',
        'midas',
        'clarity',
        'api',
        'psfterp',
        'optilink',
        'genius',
        'epsi2dm',
        ]


def token_query(src):
    token_query_sql = f'''
        select
            case
                when cast(dateadd(hh, 3, max(DRP_TS)) as date) = cast(getdate() as date)
                        and max(DRP_TS) < getdate() 
                    then 1
                else 0
            end
        from FI_EDW_TOKEN.dbo.EDW_TOKEN_DROPS with(nolock)
        where SRC_NM = '{src}'
    '''

    return token_query_sql


poke_int = 600

with DAG('test_token_drop', default_args=default_args, catchup=False, schedule_interval='15 0 * * *') as dag:
    # # define sensor tasks
    # clarity_fresh = SqlSensor(
    #         task_id='clarity_fresness',
    #         conn_id='ebi_datamart',
    #         sql=token_query('CLARITY'),
    #         pool='default_pool',
    #         poke_interval=poke_int,
    #         mode='reschedule',
    #         dag=dag,
    #         )
    #
    # epsi_fresh = SqlSensor(
    #         task_id='epsi_fresness',
    #         conn_id='ebi_datamart',
    #         sql=token_query('EPSI2DM'),
    #         pool='default_pool',
    #         poke_interval=poke_int,
    #         mode='reschedule',
    #         dag=dag,
    #         )
    #
    # morrisey_fresh = SqlSensor(
    #         task_id='morrisey_fresness',
    #         conn_id='ebi_datamart',
    #         sql=token_query('MOR2DM'),
    #         pool='default_pool',
    #         poke_interval=poke_int,
    #         mode='reschedule',
    #         dag=dag,
    #         )
    #
    # # define dummy transform tasks
    # clarity_trans = DummyOperator(
    #         task_id='clarity_only_transform',
    #         pool='default_pool',
    #         dag=dag,
    #         )
    #
    # clarity_epsi_trans = DummyOperator(
    #         task_id='clarity_plus_epsi_transform',
    #         pool='default_pool',
    #         dag=dag,
    #         )
    #
    # clarity_morr_trans = DummyOperator(
    #         task_id='clarity_plus_morrisey_transform',
    #         pool='default_pool',
    #         dag=dag,
    #         )
    #
    # # order of operations
    # clarity_fresh >> clarity_trans
    # clarity_fresh >> clarity_epsi_trans
    # clarity_fresh >> clarity_morr_trans
    # epsi_fresh >> clarity_epsi_trans
    # morrisey_fresh >> clarity_morr_trans

    # define sensor tasks and assign them to dict
    sensor_dict = {}
    for src in srcs:
        sensor = SqlSensor(
                task_id=f'{src}_freshness',
                conn_id='ebi_datamart',
                sql=token_query(src.upper()),  # upper is how they're stored in DB
                pool='default_pool',
                poke_interval=poke_int,
                mode='reschedule',
                dag=dag,
                )
        sensor_dict[src] = sensor

    # get combinations of each pair of sources
    combs = list(combinations(srcs, 2))

    # create dummies for each pair and set them downstream of sensors
    for comb in combs:
        dummy = DummyOperator(
                task_id=f'{comb[0]}_{comb[1]}_transform',
                pool='default_pool',
                dag=dag,
                )
        [sensor_dict[comb[0]], sensor_dict[comb[1]]] >> dummy
