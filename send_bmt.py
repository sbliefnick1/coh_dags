from datetime import datetime, timedelta

import pendulum

from airflow import DAG
from airflow.operators.email_operator import EmailOperator
from airflow.operators.sensors import ExternalTaskSensor
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.models import Variable

from auxiliary.outils import get_json_secret

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 7, 29, tzinfo=pendulum.timezone('America/Los_Angeles')),
    'email': ['sbliefnick@coh.org'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=3)
    }

dag = DAG('send_bmt', default_args=default_args, catchup=False, schedule_interval='0 12 * * 1')

pw = get_json_secret('ebi_db_conn')['db_connections']['fi_dm_ebi']['password']

tasks_to_wait_for = [
    'fact_hospital_transaction_clarity',
    'dim_hospital_account_clarity',
    'dim_guarantor_clarity',
    'bridge_hospital_account_coverage_clarity',
    'dim_payor_plan_clarity',
    'dim_patient_clarity'
    ]

tasks = []
for t in tasks_to_wait_for:
    task = ExternalTaskSensor(external_dag_id='run_master_etl',
                              external_task_id='exec_ebi_{}_logic'.format(t),
                              execution_delta=timedelta(days=-6, hours=7, minutes=20),
                              task_id='wait_for_{}'.format(t),
                              dag=dag)
    tasks.append(task)

path = 'C:\\Airflow\\send_bmt'
ebi_db_server_prod = Variable.get('ebi_db_server_prod')
airflow_server_prod = Variable.get('airflow_server_prod')

# -S server, -d database
# -E trusted connection, -i input file
# -o output file, -s, use comma to separate fields
# -W trim white space, -X security measure for automated envs
query_cmd = (f'sqlcmd -S {ebi_db_server_prod} -d FI_DM_EBI -E '
             f'-i {path}\\bmt_query.sql '
             f'-o {path}\\bmt_results.csv '
             '-s, -W -X')

copy_cmd = (f'pscp -pw {pw} {path}\\bmt_results.csv '
            f'{airflow_server_prod}:/var/nfsshare/files')

query = SSHOperator(ssh_conn_id='tableau_server',
                    task_id='query_bmt',
                    command=query_cmd,
                    dag=dag)

copy = SSHOperator(ssh_conn_id='tableau_server',
                   task_id='copy_bmt',
                   command=copy_cmd,
                   dag=dag)

email = EmailOperator(task_id='email_bmt',
                      to=['chowens@coh.org'],
                      cc=['ddeaville@coh.org'],
                      subject='BMT Data {{ ds }}',
                      html_content='See attached.',
                      files=['/var/nfsshare/files/bmt_results.csv'],
                      dag=dag
                      )

tasks >> query >> copy >> email
