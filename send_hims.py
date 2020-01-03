from datetime import datetime, timedelta

import pendulum

from airflow import DAG
from airflow.operators.email_operator import EmailOperator
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.models import Variable

from auxiliary.outils import get_json_secret

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 9, 30, tzinfo=pendulum.timezone('America/Los_Angeles')),
    'email': ['sbliefnick@coh.org'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
    }

dag = DAG('send_hims', default_args=default_args, catchup=False, schedule_interval='0 9 * * 1')

pw = get_json_secret('ebi_db_conn')['db_connections']['fi_dm_ebi']['password']

path = 'C:\\Airflow\\send_hims'
ebi_db_server_prod = Variable.get('ebi_db_server_prod')
airflow_server_prod = Variable.get('airflow_server_prod')

query_cmd = (f'sqlcmd -S {ebi_db_server_prod} -d FI_DM_EBI -E '
             f'-i {path}\\hims_query.sql '
             f'-o {path}\\hims_results.csv '
             '-s"|" -W -X -I')

copy_cmd = (f'pscp -pw {pw} {path}\\hims_results.csv '
            f'{airflow_server_prod}:/var/nfsshare/files')

query = SSHOperator(ssh_conn_id='tableau_server',
                    task_id='query_hims',
                    command=query_cmd,
                    dag=dag)

copy = SSHOperator(ssh_conn_id='tableau_server',
                   task_id='copy_hims',
                   command=copy_cmd,
                   dag=dag)

email = EmailOperator(task_id='email_hims',
                      to=['wgoicochea@coh.org', 'jcanaber@coh.org', 'shrogers@coh.org'],
                      cc=['ddeaville@coh.org'],
                      subject='HIMS Data {{ ds }}',
                      html_content='See attached.',
                      files=['/var/nfsshare/files/hims_results.csv'],
                      dag=dag
                      )

query >> copy >> email
