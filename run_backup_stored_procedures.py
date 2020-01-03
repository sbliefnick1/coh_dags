from datetime import datetime, timedelta

import pendulum

from airflow import DAG
from airflow.contrib.operators.ssh_operator import SSHOperator


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 5, 20, tzinfo=pendulum.timezone('America/Los_Angeles')),
    'email': ['sbliefnick@coh.org', 'jharris@coh.org'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
    }

dag = DAG('run_backup_stored_procedures', default_args=default_args, catchup=False, schedule_interval='0 21 * * *')

t1_bash = 'python C:\\Anaconda\\ETL\\fi_dm_ebi\\backup_stored_procedures.py'

t1 = SSHOperator(ssh_conn_id='tableau_server',
                 task_id='run_backup',
                 command=t1_bash,
                 dag=dag)

# backup_stored_procedures.py

# import textwrap
#
# import pandas as pd
# import sqlalchemy as sa
#
# from config.get_salesforce_config import connection_string
#
#
# def get_procedure_logic(proc_id, name, today_date):
#     codesql = 'select definition from sys.sql_modules where object_id = {}'.format(proc_id)
#     code = pd.read_sql(codesql, ppw_engine)['definition'][0]
#     file = name + today_date + ".sql"
#     filepath = directory + file
#     with open(filepath, 'w') as f:
#         f.write(code)
#
#
# def iterate_over_procs(dataframe):
#     for index, row in dataframe.iterrows():
#         proc_id = row['object_id']
#         proc_name = row['name']
#         today = row['date']
#         get_procedure_logic(proc_id, proc_name, today)
#
#
# if __name__ == '__main__':
#     directory = "\\\\fs1\\everyone\\ebi\\ETL\\Stored Procedure Backups\\"
#     sql = textwrap.dedent("""\
#            select p.object_id, name, convert(varchar, modify_date, 112) as date
#           from sys.procedures p
#           where name like 'EBI[_]%[_]Logic'
#           and cast(modify_date as date) >= dateadd(day, -1, getdate())""")
#
#     ppw_engine = sa.create_engine(connection_string)
#     df = pd.read_sql(sql, ppw_engine)
#     iterate_over_procs(df)
