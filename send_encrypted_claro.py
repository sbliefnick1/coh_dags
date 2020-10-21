from datetime import datetime, timedelta
from pathlib import Path

import pendulum

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.contrib.operators.sftp_operator import SFTPOperator
from airflow.models import Variable

from auxiliary.outils import get_json_secret

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 10, 15, tzinfo=pendulum.timezone('America/Los_Angeles')),
    'email': ['sbliefnick@coh.org'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
    }

dag = DAG('send_encrypted_claro', default_args=default_args, catchup=False, schedule_interval='0 9 15 * *')

pw = get_json_secret('ebi_db_conn')['db_connections']['fi_dm_ebi']['password']
basepath = Path('/var/nfsshare/gpg')
output_file = 'Claro_Patient_Account_File_Extract_{{ ds_nodash }}.txt'
output_path = f'C:\\Airflow\\claro\\{output_file}'
claro_server = Variable.get('claro_server')
airflow_server_prod = Variable.get('airflow_server_prod')

clear_cmd = 'rm -rf /var/nfsshare/gpg/Claro_Patient_Account_File_Extract_*'

# -l 30 raises login timeout since it seems to be finicky
query_cmd = (f'sqlcmd -S {claro_server} -d Clarity_PRD_Report '
             f'-Q "set nocount on; exec coh.sp_Claro_Patient_Account_File_Extract; select * from coh.Claro_Patient_Account_File" '
             f'-o {output_path} '
             f'-s"|" -W -X -I -l 30')

copy_cmd = f'pscp -pw {pw} {output_path} {airflow_server_prod}:{basepath}'

encrypt_cmd = (f"gpg --homedir {basepath}/.gnupg  --encrypt --batch --yes --trust-model always -r "
               f"claro2020@clarohealthcare.com {basepath}/{output_file}")

clear = BashOperator(task_id='clear_old_files',
                     bash_command=clear_cmd,
                     dag=dag)

query = SSHOperator(ssh_conn_id='tableau_server',
                    task_id='query_claro',
                    command=query_cmd,
                    dag=dag)

copy = SSHOperator(ssh_conn_id='tableau_server',
                   task_id='copy_claro',
                   command=copy_cmd,
                   dag=dag)

encrypt = BashOperator(task_id='encrypt_file',
                       bash_command=encrypt_cmd,
                       dag=dag)

sftp = SFTPOperator(task_id='upload_claro_to_sftp',
                    ssh_conn_id='claro_sftp',
                    local_filepath=f'{basepath}/{output_file}.gpg',
                    remote_filepath=f'/{output_file}.gpg',
                    create_intermediate_dirs=True,
                    dag=dag)

clear >> query >> copy >> encrypt >> sftp
