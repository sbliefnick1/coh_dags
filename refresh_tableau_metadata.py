from datetime import datetime, timedelta

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
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

dag = DAG('refresh_tableau_metadata', default_args=default_args, catchup=False, schedule_interval='0 20 * * *')

users_bash = 'cd C:\\Anaconda\\ETL\\tableau && python tableau_users.py'
system_users_bash = 'cd C:\\Anaconda\\ETL\\tableau && python tableau_system_users.py'
views_bash = 'cd C:\\Anaconda\\ETL\\tableau && python tableau_views.py'
workbooks_bash = 'cd C:\\Anaconda\\ETL\\tableau && python tableau_workbooks.py'
sites_bash = 'cd C:\\Anaconda\\ETL\\tableau && python tableau_sites.py'
datasources_bash = 'cd C:\\Anaconda\\ETL\\tableau && python tableau_datasources.py'
groups_bash = 'cd C:\\Anaconda\\ETL\\tableau && python tableau_groups.py'
group_users_bash = 'cd C:\\Anaconda\\ETL\\tableau && python tableau_group_users.py'
domains_bash = 'cd C:\\Anaconda\\ETL\\tableau && python tableau_domains.py'
licensing_roles_bash = 'cd C:\\Anaconda\\ETL\\tableau && python tableau_licensing_roles.py'
table_assets_bash = 'cd C:\\Anaconda\\ETL\\tableau && python tableau_table_assets.py'
table_asset_sources_bash = 'cd C:\\Anaconda\\ETL\\tableau && python tableau_table_asset_sources.py'
database_assets_bash = 'cd C:\\Anaconda\\ETL\\tableau && python tableau_database_assets.py'
datasource_tables_bash = 'cd C:\\Anaconda\\ETL\\tableau && python TableauServerDSTables.py'
workbook_datasources_bash = 'cd C:\\Anaconda\\ETL\\tableau && python tableau_workbook_datasources.py'
customized_views_bash = 'cd C:\\Anaconda\\ETL\\tableau && python tableau_customized_views.py'
subscriptions_bash = 'cd C:\\Anaconda\\ETL\\tableau && python tableau_subscriptions.py'

tps = PythonOperator(
        task_id='refresh_tableau_permissions_stats',
        python_callable=refresh_tableau_extract,
        op_kwargs={'datasource_id': '78984f9a-f731-4e24-8379-7c992a88029e'},
        dag=dag
        )

tus = PythonOperator(
        task_id='refresh_tableau_usage_stats',
        python_callable=refresh_tableau_extract,
        op_kwargs={'datasource_id': '733c626f-2729-479a-8cb6-d953fbeaed40'},
        dag=dag
        )

u = SSHOperator(ssh_conn_id='tableau_server',
                task_id='tableau_users',
                command=users_bash,
                dag=dag)

su = SSHOperator(ssh_conn_id='tableau_server',
                 task_id='tableau_system_users',
                 command=system_users_bash,
                 dag=dag)

v = SSHOperator(ssh_conn_id='tableau_server',
                task_id='tableau_views',
                command=views_bash,
                dag=dag)

w = SSHOperator(ssh_conn_id='tableau_server',
                task_id='tableau_workbooks',
                command=workbooks_bash,
                dag=dag)

s = SSHOperator(ssh_conn_id='tableau_server',
                task_id='tableau_sites',
                command=sites_bash,
                dag=dag)

d = SSHOperator(ssh_conn_id='tableau_server',
                task_id='tableau_datasources',
                command=datasources_bash,
                dag=dag)

g = SSHOperator(ssh_conn_id='tableau_server',
                task_id='tableau_groups',
                command=groups_bash,
                dag=dag)

gu = SSHOperator(ssh_conn_id='tableau_server',
                 task_id='tableau_group_users',
                 command=group_users_bash,
                 dag=dag)

dm = SSHOperator(ssh_conn_id='tableau_server',
                 task_id='tableau_domains',
                 command=domains_bash,
                 dag=dag)

lr = SSHOperator(ssh_conn_id='tableau_server',
                 task_id='tableau_licensing_roles',
                 command=licensing_roles_bash,
                 dag=dag)

ta = SSHOperator(ssh_conn_id='tableau_server',
                 task_id='tableau_table_assets',
                 command=table_assets_bash,
                 dag=dag)

tas = SSHOperator(ssh_conn_id='tableau_server',
                  task_id='tableau_table_asset_sources',
                  command=table_asset_sources_bash,
                  dag=dag)

da = SSHOperator(ssh_conn_id='tableau_server',
                 task_id='tableau_database_assets',
                 command=database_assets_bash,
                 dag=dag)

dt = SSHOperator(ssh_conn_id='tableau_server',
                 task_id='tableau_metadata_datasource_tables',
                 command=datasource_tables_bash,
                 dag=dag)

wd = SSHOperator(ssh_conn_id='tableau_server',
                 task_id='tableau_workbook_datasources',
                 command=workbook_datasources_bash,
                 dag=dag)

wdr = PythonOperator(
        task_id='refresh_workbook_datasources',
        python_callable=refresh_tableau_extract,
        op_kwargs={'datasource_id': '14f2e7b9-0b58-4e08-8a53-27d7a9817248'},
        dag=dag
        )

cv = SSHOperator(ssh_conn_id='tableau_server',
                 task_id='tableau_customized_views',
                 command=customized_views_bash,
                 dag=dag)

sb = SSHOperator(ssh_conn_id='tableau_server',
                 task_id='tableau_subscriptions',
                 command=subscriptions_bash,
                 dag=dag)

tps >> tus
u >> su >> v >> w >> s >> d >> g >> gu >> dm >> lr >> ta >> tas >> da >> cv >> sb >> dt >> wd
wd >> wdr
