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

dag = DAG('run_mor_etl', default_args=default_args, catchup=False, schedule_interval='0 8 * * *')

conn_id = 'ebi_datamart'
pool_id = 'ebi_etl_pool'

# deps = ExternalTaskSensor(
#        external_dag_id='run_daily_census',
#        external_task_id='refresh_daily_census',
#        task_id='wait_for_daily_census',
#        dag=dag
#        )

RTE = MsSqlOperator(
        sql='EXEC EBI_MOR_RevtoExp_CostCenters_Logic;',
        task_id='mor_rev_to_exp_cost_centers',
        autocommit=True,
        mssql_conn_id=conn_id,
        pool=pool_id,
        dag=dag
        )

BFB = MsSqlOperator(
        sql='EXEC EBI_MOR_BPC_Flex_Budget_Logic;',
        task_id='mor_bpc_flex_budget',
        autocommit=True,
        mssql_conn_id=conn_id,
        pool=pool_id,
        dag=dag
        )

MSS = MsSqlOperator(
        sql='EXEC EBI_MOR_Summary_wSecurity_Logic;',
        task_id='mor_summary_w_security',
        autocommit=True,
        mssql_conn_id=conn_id,
        pool=pool_id,
        dag=dag
        )

MSSR = PythonOperator(
        task_id='refresh_mor_summary_w_security',
        python_callable=refresh_tableau_extract,
        op_kwargs={'datasource_id': '75834DFA-3C69-4C65-A6DF-BACE3E850B10'},
        dag=dag
        )

MA = MsSqlOperator(
        sql='EXEC EBI_MOR_Account_Logic;',
        task_id='ebi_mor_account',
        autocommit=True,
        mssql_conn_id=conn_id,
        pool=pool_id,
        dag=dag
        )

MTS = MsSqlOperator(
        sql='EXEC EBI_MOR_Transaction_wSecurity_Logic;',
        task_id='mor_transaction_w_security',
        autocommit=True,
        mssql_conn_id=conn_id,
        pool=pool_id,
        dag=dag
        )

MTSR = PythonOperator(
        task_id='refresh_mor_transaction_w_security',
        python_callable=refresh_tableau_extract,
        op_kwargs={'datasource_id': '61705DAD-5538-46B8-A1EA-4F5636D3E9E8'},
        dag=dag
        )

MFR = PythonOperator(
        task_id='refresh_mor_fte_w_security',
        python_callable=refresh_tableau_extract,
        op_kwargs={'datasource_id': '939E222D-D494-4CDD-BF99-E28F07151FE5'},
        dag=dag
        )

CE = MsSqlOperator(
        sql='EXEC EBI_COH_Employees_Logic;',
        task_id='coh_employees',
        autocommit=True,
        mssql_conn_id=conn_id,
        pool=pool_id,
        dag=dag
        )

CPS = MsSqlOperator(
        sql='EXEC EBI_COH_Accounting_CapitalProject_Security_Logic;',
        task_id='accounting_capital_project_security',
        autocommit=True,
        mssql_conn_id=conn_id,
        pool=pool_id,
        dag=dag
        )

CP = MsSqlOperator(
        sql='EXEC EBI_COH_Accounting_CapitalProject_Logic;',
        task_id='accounting_capital_project',
        autocommit=True,
        mssql_conn_id=conn_id,
        pool=pool_id,
        dag=dag
        )

CPR = PythonOperator(
        task_id='refresh_accounting_capital_project',
        python_callable=refresh_tableau_extract,
        op_kwargs={'datasource_id': 'EBFD6487-18D1-4FF7-AB1F-8729D13C945A'},
        dag=dag
        )

MBRR = PythonOperator(
        task_id='refresh_mor_budget_rollup_account_category',
        python_callable=refresh_tableau_extract,
        op_kwargs={'datasource_id': '96916869-534E-4C08-BDF0-A8863A6F2C74'},
        dag=dag
        )

# deps >> RTE
# deps >> MA
# deps >> ML
# deps >> MF
# deps >> CE
# deps >> CPS
RTE >> BFB
MA >> BFB
MA >> MTS
BFB >> MSS
MSS >> MSSR
MSS >> MBRR
MTS >> MTSR
CPS >> CP
CP >> CPR
