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

dag = DAG('refresh_scm', default_args=default_args, catchup=False, schedule_interval='0 16 * * *')

conn_id = 'ebi_datamart'
pool_id = 'ebi_etl_pool'

si = MsSqlOperator(
        sql='EXEC EBI_SCM_Items_Logic;',
        task_id='load_scm_items',
        autocommit=True,
        mssql_conn_id=conn_id,
        pool=pool_id,
        dag=dag
        )

sv = MsSqlOperator(
        sql='EXEC EBI_SCM_Vendors_Logic;',
        task_id='load_scm_vendors',
        autocommit=True,
        mssql_conn_id=conn_id,
        pool=pool_id,
        dag=dag
        )

vch = MsSqlOperator(
        sql='EXEC EBI_SCM_Vouchers_Logic;',
        task_id='load_scm_vouchers',
        autocommit=True,
        mssql_conn_id=conn_id,
        pool=pool_id,
        dag=dag
        )

vchw = MsSqlOperator(
        sql='EXEC EBI_SCM_Voucher_WO_AcctngLn_Logic;',
        task_id='load_scm_voucher_wo_acctngln',
        autocommit=True,
        mssql_conn_id=conn_id,
        pool=pool_id,
        dag=dag
        )

sr = MsSqlOperator(
        sql='EXEC EBI_SCM_Receipts_Logic;',
        task_id='load_scm_receipts',
        autocommit=True,
        mssql_conn_id=conn_id,
        pool=pool_id,
        dag=dag
        )

srw = MsSqlOperator(
        sql='EXEC EBI_SCM_Receipts_WO_AcctngLn_Logic;',
        task_id='load_scm_receipts_wo_acctngln',
        autocommit=True,
        mssql_conn_id=conn_id,
        pool=pool_id,
        dag=dag
        )

spo = MsSqlOperator(
        sql='EXEC EBI_SCM_Purchase_Orders_Logic;',
        task_id='load_scm_purchase_orders',
        autocommit=True,
        mssql_conn_id=conn_id,
        pool=pool_id,
        dag=dag
        )

sit = MsSqlOperator(
        sql='EXEC EBI_SCM_Inventory_Transactions_Logic;',
        task_id='load_scm_inv_transaction',
        autocommit=True,
        mssql_conn_id=conn_id,
        pool=pool_id,
        dag=dag
        )

sp = MsSqlOperator(
        sql='EXEC EBI_SCM_Payments_Logic;',
        task_id='load_scm_payments',
        autocommit=True,
        mssql_conn_id=conn_id,
        pool=pool_id,
        dag=dag
        )

spc = MsSqlOperator(
        sql='EXEC EBI_SCM_PCards_Logic;',
        task_id='load_scm_pcards',
        autocommit=True,
        mssql_conn_id=conn_id,
        pool=pool_id,
        dag=dag
        )

srt = MsSqlOperator(
        sql='EXEC EBI_SCM_Returns_Logic;',
        task_id='load_scm_returns',
        autocommit=True,
        mssql_conn_id=conn_id,
        pool=pool_id,
        dag=dag
        )

srq = MsSqlOperator(
        sql='EXEC EBI_SCM_Requisitions_Logic;',
        task_id='load_scm_requisitions',
        autocommit=True,
        mssql_conn_id=conn_id,
        pool=pool_id,
        dag=dag
        )

alv = MsSqlOperator(
        sql='EXEC EBI_SCM_AcctgLn_Voucher_Logic;',
        task_id='load_scm_acctgln_voucher',
        autocommit=True,
        mssql_conn_id=conn_id,
        pool=pool_id,
        dag=dag
        )

jl = MsSqlOperator(
        sql='EXEC EBI_GL_Journal_Line_Logic;',
        task_id='load_gl_journal_line',
        autocommit=True,
        mssql_conn_id=conn_id,
        pool=pool_id,
        dag=dag
        )

jlr = MsSqlOperator(
        sql='EXEC EBI_GL_Journal_Line_Recv_Logic;',
        task_id='load_gl_journal_line_recv',
        autocommit=True,
        mssql_conn_id=conn_id,
        pool=pool_id,
        dag=dag
        )

jlv = MsSqlOperator(
        sql='EXEC EBI_GL_Journal_Line_Vchr_Logic;',
        task_id='load_gl_journal_line_vchr',
        autocommit=True,
        mssql_conn_id=conn_id,
        pool=pool_id,
        dag=dag
        )

jlcm = MsSqlOperator(
        sql='EXEC EBI_GL_Journal_Line_CostMgmt_Logic;',
        task_id='load_gl_journal_line_costmgmt',
        autocommit=True,
        mssql_conn_id=conn_id,
        pool=pool_id,
        dag=dag
        )

jlc = MsSqlOperator(
        sql='EXEC EBI_GL_Journal_Line_Complete_Logic;',
        task_id='load_gl_journal_line_complete',
        autocommit=True,
        mssql_conn_id=conn_id,
        pool=pool_id,
        dag=dag
        )

alr = MsSqlOperator(
        sql='EXEC EBI_SCM_AcctgLn_Receiving_Logic;',
        task_id='load_scm_acctgln_receiving',
        autocommit=True,
        mssql_conn_id=conn_id,
        pool=pool_id,
        dag=dag
        )

alcm = MsSqlOperator(
        sql='EXEC EBI_SCM_AcctgLn_CostManagement_Logic;',
        task_id='load_scm_acctgln_costmanagement',
        autocommit=True,
        mssql_conn_id=conn_id,
        pool=pool_id,
        dag=dag
        )

alb = MsSqlOperator(
        sql='EXEC EBI_SCM_AcctgLn_BUD_SJI_Logic;',
        task_id='load_scm_acctgln_bud_sji',
        autocommit=True,
        mssql_conn_id=conn_id,
        pool=pool_id,
        dag=dag
        )

alss = MsSqlOperator(
        sql='EXEC EBI_SCM_AcctgLn_Supply_Spend_Logic;',
        task_id='load_scm_acctgln_supply_spend',
        autocommit=True,
        mssql_conn_id=conn_id,
        pool=pool_id,
        dag=dag
        )

sse = PythonOperator(
        task_id='refresh_scm_acctgline_supply_spend',
        python_callable=refresh_tableau_extract,
        op_kwargs={'datasource_id': 'd10c10a6-22c0-45c9-8a00-867cc32254e1'},
        dag=dag
        )

spe = PythonOperator(
        task_id='refresh_scm_pcards',
        python_callable=refresh_tableau_extract,
        op_kwargs={'datasource_id': '21d7aa56-2bd1-4fec-9602-f54236178524'},
        dag=dag
        )

pce = PythonOperator(
        task_id='refresh_scm_pcard_comments',
        python_callable=refresh_tableau_extract,
        op_kwargs={'datasource_id': '5c9d870e-b05e-4213-adfa-0d22bb6505a4'},
        dag=dag
        )

scws = PythonOperator(
        task_id='refresh_scm_cspt_warehouse_stock',
        python_callable=refresh_tableau_extract,
        op_kwargs={'datasource_id': '51db78e8-b319-4b8d-9c59-b4758cefdd9b'},
        dag=dag
        )

scmi = PythonOperator(
        task_id='refresh_scm_inventory',
        python_callable=refresh_tableau_extract,
        op_kwargs={'datasource_id': 'c5c47779-a321-48ee-a12f-1f4c933f26c6'},
        dag=dag
        )

scmiu = PythonOperator(
        task_id='refresh_scm_inventory_usage',
        python_callable=refresh_tableau_extract,
        op_kwargs={'datasource_id': '9737e41f-1e28-444a-8f70-0d9ee47569ec'},
        dag=dag
        )

scmibu = PythonOperator(
        task_id='refresh_scm_inventory_business_units',
        python_callable=refresh_tableau_extract,
        op_kwargs={'datasource_id': '1da002a9-25fd-4139-b4d1-e3244ef919fb'},
        dag=dag
        )

scmic = PythonOperator(
        task_id='refresh_scm_inventory_count',
        python_callable=refresh_tableau_extract,
        op_kwargs={'datasource_id': 'f046bc66-4b42-47f7-b074-243b48c49d06'},
        dag=dag
        )

scmp = PythonOperator(
        task_id='refresh_scm_procurement_spend',
        python_callable=refresh_tableau_extract,
        op_kwargs={'datasource_id': 'e97743f2-eac5-4553-ab38-99989498bd16'},
        dag=dag
        )

scmid = PythonOperator(
        task_id='refresh_scm_inventory_demand',
        python_callable=refresh_tableau_extract,
        op_kwargs={'datasource_id': '834a01c3-ea84-4cac-8a9d-f63acbf9f84a'},
        dag=dag
        )

scmta = PythonOperator(
        task_id='refresh_scm_transport_activity',
        python_callable=refresh_tableau_extract,
        op_kwargs={'datasource_id': 'fee39e73-5d2d-480d-9e40-be594d08ed7a'},
        dag=dag
        )

scmmmpa = PythonOperator(
        task_id='refresh_scm_materials_management_price_analysis',
        python_callable=refresh_tableau_extract,
        op_kwargs={'datasource_id': '1ba18b72-6aea-4734-a49c-3e64b9849643'},
        dag=dag
        )

scmpar = PythonOperator(
        task_id='refresh_scm_par_analysis',
        python_callable=refresh_tableau_extract,
        op_kwargs={'datasource_id': '638bac72-6b6e-4400-998e-53c3657cb7fe'},
        dag=dag
        )


si >> sv
si >> scws
si >> scmi
si >> scmiu
si >> scmibu
si >> scmic
si >> scmid
si >> scmta
si >> scmmmpa
si >> scmpar

sv >> vch
sv >> vchw
sv >> sr
sv >> srw
sv >> spo
sv >> sit
sv >> sp
sv >> spc
sv >> srt
sv >> srq
sv >> scmp

jl >> jlr >> jlv >> jlcm >> jlc

jlc >> alv
vch >> alv
vchw >> alv
sr >> alv
srw >> alv
spo >> alv
sit >> alv
sp >> alv
spc >> alv
srt >> alv
srq >> alv

alv >> alr >> alcm >> alb >> alss

alb >> alss >> sse
alb >> spe
alb >> pce
