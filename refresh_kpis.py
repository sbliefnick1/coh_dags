from datetime import datetime, timedelta

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator
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

dag = DAG('refresh_kpis', default_args=default_args, catchup=False, concurrency=2, schedule_interval='0 19 * * *')

conn_id = 'ebi_datamart'
pool_id = 'ebi_etl_pool'

awt = MsSqlOperator(
        sql='EXEC sp_EBI_KPI_AGG_Adjusted_Wait_Time;',
        task_id='adjusted_wait_time',
        autocommit=True,
        mssql_conn_id=conn_id,
        pool=pool_id,
        dag=dag
        )

ca = MsSqlOperator(
        sql='EXEC sp_EBI_KPI_AGG_Call_Abandonment;',
        task_id='calls_abandoned',
        autocommit=True,
        mssql_conn_id=conn_id,
        pool=pool_id,
        dag=dag
        )

cfd = MsSqlOperator(
        sql='EXEC sp_EBI_KPI_AGG_Cancel_14;',
        task_id='cancel_14_days',
        autocommit=True,
        mssql_conn_id=conn_id,
        pool=pool_id,
        dag=dag
        )

cv = MsSqlOperator(
        sql='EXEC sp_EBI_KPI_AGG_Clinic_Volume;',
        task_id='clinic_volume',
        autocommit=True,
        mssql_conn_id=conn_id,
        pool=pool_id,
        dag=dag
        )

cfr = MsSqlOperator(
        sql='EXEC sp_EBI_KPI_AGG_Cancel_14_Rate;',
        task_id='cancel_14_rate',
        autocommit=True,
        mssql_conn_id=conn_id,
        pool=pool_id,
        dag=dag
        )

cvp = MsSqlOperator(
        sql='EXEC sp_EBI_KPI_AGG_Clinic_Volume_Percent;',
        task_id='clinic_volume_percent',
        autocommit=True,
        mssql_conn_id=conn_id,
        pool=pool_id,
        dag=dag
        )

opr = MsSqlOperator(
        sql='EXEC sp_EBI_KPI_AGG_OP_Recommend;',
        task_id='op_recommend',
        autocommit=True,
        mssql_conn_id=conn_id,
        pool=pool_id,
        dag=dag
        )

sts = MsSqlOperator(
        sql='EXEC sp_EBI_KPI_AGG_Sched_to_Seen;',
        task_id='scheduled_to_seen',
        autocommit=True,
        mssql_conn_id=conn_id,
        pool=pool_id,
        dag=dag
        )

oero = MsSqlOperator(
        sql='EXEC sp_EBI_KPI_AGG_OP_Ease_Reach_Office;',
        task_id='op_ease_reach_office',
        autocommit=True,
        mssql_conn_id=conn_id,
        pool=pool_id,
        dag=dag
        )

oeor = MsSqlOperator(
        sql='EXEC sp_EBI_KPI_AGG_OP_Ease_of_Reg;',
        task_id='op_ease_of_registration',
        autocommit=True,
        mssql_conn_id=conn_id,
        pool=pool_id,
        dag=dag
        )

owcs = MsSqlOperator(
        sql='EXEC sp_EBI_KPI_AGG_OP_Wait_Call_Sched;',
        task_id='op_wait_call_to_sched',
        autocommit=True,
        mssql_conn_id=conn_id,
        pool=pool_id,
        dag=dag
        )

octm = MsSqlOperator(
        sql='EXEC sp_EBI_KPI_AGG_OP_Care_Team;',
        task_id='op_care_team',
        autocommit=True,
        mssql_conn_id=conn_id,
        pool=pool_id,
        dag=dag
        )

cta = MsSqlOperator(
        sql='EXEC sp_EBI_KPI_AGG_Clin_Third_Avail;',
        task_id='clinic_third_available',
        autocommit=True,
        mssql_conn_id=conn_id,
        pool=pool_id,
        dag=dag
        )

im = MsSqlOperator(
        sql='EXEC sp_EBI_KPI_AGG_IB_Messages;',
        task_id='ib_messages',
        autocommit=True,
        mssql_conn_id=conn_id,
        pool=pool_id,
        dag=dag
        )

rta = MsSqlOperator(
        sql='EXEC sp_EBI_KPI_AGG_Rad_Third_Avail;',
        task_id='rad_third_available',
        autocommit=True,
        mssql_conn_id=conn_id,
        pool=pool_id,
        dag=dag
        )

sle = PythonOperator(
        task_id='refresh_service_line_extract',
        python_callable=refresh_tableau_extract,
        op_kwargs={'datasource_id': '486e62e8-d87c-45d5-a4ff-e72cc13c8d4d'},
        dag=dag
        )

aar = MsSqlOperator(
        sql='EXEC sp_EBI_Access_KPI_AGG_Abandon_Rate;',
        task_id='access_abandon_rate',
        autocommit=True,
        mssql_conn_id=conn_id,
        pool=pool_id,
        dag=dag
        )

aaa = MsSqlOperator(
        sql='EXEC sp_EBI_Access_KPI_AGG_Add_On_Appts;',
        task_id='access_add_on_appts',
        autocommit=True,
        mssql_conn_id=conn_id,
        pool=pool_id,
        dag=dag
        )

aaau = MsSqlOperator(
        sql='EXEC sp_EBI_Access_KPI_AGG_Appointment_Authorization;',
        task_id='access_appt_authorization',
        autocommit=True,
        mssql_conn_id=conn_id,
        pool=pool_id,
        dag=dag
        )

aaos = MsSqlOperator(
        sql='EXEC sp_EBI_Access_KPI_AGG_Appt_Order_to_Sched;',
        task_id='access_appt_ordered_to_scheduled',
        autocommit=True,
        mssql_conn_id=conn_id,
        pool=pool_id,
        dag=dag
        )

acs = MsSqlOperator(
        sql='EXEC sp_EBI_Access_KPI_AGG_Called_to_Scheduled;',
        task_id='access_called_to_scheduled',
        autocommit=True,
        mssql_conn_id=conn_id,
        pool=pool_id,
        dag=dag
        )

acts = MsSqlOperator(
        sql='EXEC sp_EBI_Access_KPI_AGG_Called_to_Seen;',
        task_id='access_called_to_seen',
        autocommit=True,
        mssql_conn_id=conn_id,
        pool=pool_id,
        dag=dag
        )

acr = MsSqlOperator(
        sql='EXEC sp_EBI_Access_KPI_AGG_Cancel_Rate;',
        task_id='access_cancel_rate',
        autocommit=True,
        mssql_conn_id=conn_id,
        pool=pool_id,
        dag=dag
        )

apr = MsSqlOperator(
        sql='EXEC sp_EBI_Access_KPI_AGG_PG_Registration;',
        task_id='access_pg_registration',
        autocommit=True,
        mssql_conn_id=conn_id,
        pool=pool_id,
        dag=dag
        )

apsv = MsSqlOperator(
        sql='EXEC sp_EBI_Access_KPI_AGG_PG_Scheduling_Your_Visit;',
        task_id='access_pg_sched_your_visit',
        autocommit=True,
        mssql_conn_id=conn_id,
        pool=pool_id,
        dag=dag
        )

asts = MsSqlOperator(
        sql='EXEC sp_EBI_Access_KPI_AGG_Sched_to_Seen;',
        task_id='access_sched_to_seen',
        autocommit=True,
        mssql_conn_id=conn_id,
        pool=pool_id,
        dag=dag
        )

asa = MsSqlOperator(
        sql='EXEC sp_EBI_Access_KPI_AGG_Surgery_Authorization;',
        task_id='access_surgery_auth',
        autocommit=True,
        mssql_conn_id=conn_id,
        pool=pool_id,
        dag=dag
        )

ata = MsSqlOperator(
        sql='EXEC sp_EBI_Access_KPI_AGG_Third_Avail;',
        task_id='access_third_available',
        autocommit=True,
        mssql_conn_id=conn_id,
        pool=pool_id,
        dag=dag
        )

ase = PythonOperator(
        task_id='refresh_access_kpi_extract',
        python_callable=refresh_tableau_extract,
        op_kwargs={'datasource_id': '9177826d-8348-4064-a645-69a64db54c77'},
        dag=dag
        )

awt >> sle
ca >> sle
cfd >> cfr >> sle
cv >> cfr >> sle
cv >> cvp >> sle
opr >> sle
sts >> sle
oero >> sle
oeor >> sle
owcs >> sle
octm >> sle
cta >> sle
im >> sle
rta >> sle

aar >> ase
aaa >> ase
aaau >> ase
aaos >> ase
acs >> ase
acts >> ase
acr >> ase
apr >> ase
apsv >> ase
asts >> ase
asa >> ase
ata >> ase
