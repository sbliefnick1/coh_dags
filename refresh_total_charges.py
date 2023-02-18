from datetime import datetime, timedelta

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
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

dag = DAG('refresh_total_charges', default_args=default_args, catchup=False, schedule_interval='0 17 * * *')

bash = 'cd C:\\Anaconda\\ETL\\foundation && conda activate foundation && python DSS_D_Data.py'

conn_id = 'ebi_datamart'
pool_id = 'ebi_etl_pool'

dss_d = SSHOperator(
        ssh_conn_id='tableau_server',
        task_id='refresh_dss_d_data',
        command=bash,
        dag=dag
        )

rvus = PythonOperator(
        task_id='refresh_rvu_extract',
        python_callable=refresh_tableau_extract,
        op_kwargs={'datasource_id': 'c08148a1-cf27-48df-8c8f-fc29f2c77c12'},
        dag=dag
        )

tcmd = MsSqlOperator(
        sql='EXEC EBI_TC_MIN_DOS_Logic;',
        task_id='load_ebi_tc_min_dos',
        autocommit=True,
        mssql_conn_id=conn_id,
        pool=pool_id,
        dag=dag
        )

tcr = MsSqlOperator(
        sql='EXEC EBI_Total_Charges_Raw_Logic;',
        task_id='load_ebi_total_charges_raw',
        autocommit=True,
        mssql_conn_id=conn_id,
        pool=pool_id,
        dag=dag
        )

tclc = MsSqlOperator(
        sql='EXEC EBI_Total_Charges_Legacy_Charges;',
        task_id='load_ebi_total_charges_legacy_charges',
        autocommit=True,
        mssql_conn_id=conn_id,
        pool=pool_id,
        dag=dag
        )

tcpc = MsSqlOperator(
        sql='EXEC EBI_Total_Charges_PB_Charges;',
        task_id='load_ebi_total_charges_pb_charges',
        autocommit=True,
        mssql_conn_id=conn_id,
        pool=pool_id,
        dag=dag
        )

tchc = MsSqlOperator(
        sql='EXEC EBI_Total_Charges_HB_Charges;',
        task_id='load_ebi_total_charges_hb_charges',
        autocommit=True,
        mssql_conn_id=conn_id,
        pool=pool_id,
        dag=dag
        )

tcf = MsSqlOperator(
        sql='EXEC EBI_Total_Charges_Final_Logic;',
        task_id='load_ebi_total_charges_final',
        autocommit=True,
        mssql_conn_id=conn_id,
        pool=pool_id,
        dag=dag
        )

new = PythonOperator(
        task_id='refresh_new_em_provider',
        python_callable=refresh_tableau_extract,
        op_kwargs={'datasource_id': 'ea9a44b2-3f27-4560-9a00-4e056ede95bd'},
        dag=dag
        )

trj = PythonOperator(
        task_id='refresh_patient_trajectory',
        python_callable=refresh_tableau_extract,
        op_kwargs={'datasource_id': '951a76e5-e7a7-466a-a0aa-6b592bd9a370'},
        dag=dag
        )

tcc = PythonOperator(
        task_id='refresh_total_charges',
        python_callable=refresh_tableau_extract,
        op_kwargs={'datasource_id': 'ad80c2d3-3926-4516-af11-55bac64735ac'},
        dag=dag
        )

kpi = PythonOperator(
        task_id='refresh_executive_kpi',
        python_callable=refresh_tableau_extract,
        op_kwargs={'datasource_id': 'bbc4a8b9-11a6-438d-a010-5bdcc42ab2cb'},
        dag=dag
        )

cim = PythonOperator(
        task_id='refresh_cim_charges',
        python_callable=refresh_tableau_extract,
        op_kwargs={'datasource_id': '6e39fb3d-fdc1-41ee-8a2a-3f819a12fea5'},
        dag=dag
        )

dss_d >> tcmd
dss_d >> rvus

tcmd >> tcr >> tcf
tcmd >> tclc >> tcf
tcmd >> tcpc >> tcf
tcmd >> tchc >> tcf

tcf >> new
tcf >> trj
tcf >> tcc
tcf >> cim
tcf >> kpi
