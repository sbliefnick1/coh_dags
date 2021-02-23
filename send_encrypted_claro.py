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
basepath = Path('/var/nfsshare')
claro_server = Variable.get('claro_server')
airflow_server_prod = Variable.get('airflow_server_prod')

# patient
output_file_patient = 'Claro_Patient_Account_File_Extract_{{ next_ds_nodash }}.txt'
output_path_patient = f'C:\\Airflow\\claro\\{output_file_patient}'

# -l 30 raises login timeout since it seems to be finicky
# -h -1 removes header row and line of dashes underneath
query_cmd_patient = (f'sqlcmd -S {claro_server} -d Clarity_PRD_Report '
                     f'-i {Variable.get("claro_query_filepath")} '
                     f'-o {output_path_patient} '
                     f'-s"|" -W -X -I -l 30 -h -1')

copy_cmd_patient = f'pscp -pw {pw} {output_path_patient} {airflow_server_prod}:{basepath}/files'

encrypt_cmd_patient = (f"gpg --encrypt -vv --batch --yes --trust-model always -r "
                       f"claro2020@clarohealthcare.com {basepath}/files/{output_file_patient}")

query_patient = SSHOperator(ssh_conn_id='tableau_server',
                            task_id='query_claro_patient',
                            command=query_cmd_patient,
                            dag=dag)

copy_patient = SSHOperator(ssh_conn_id='tableau_server',
                           task_id='copy_claro_patient',
                           command=copy_cmd_patient,
                           dag=dag)

encrypt_patient = BashOperator(task_id='encrypt_file_patient',
                               bash_command=encrypt_cmd_patient,
                               dag=dag)

sftp_patient = SFTPOperator(task_id='upload_claro_to_sftp_patient',
                            ssh_conn_id='claro_sftp',
                            local_filepath=f'{basepath}/files/{output_file_patient}.gpg',
                            remote_filepath=f'/{output_file_patient}.gpg',
                            create_intermediate_dirs=True,
                            dag=dag)

query_patient >> copy_patient >> encrypt_patient >> sftp_patient

# physician roster
output_file_roster = 'Claro_Physician_Roster_{{ next_ds_nodash }}.txt'
output_path_roster = f'C:\\Airflow\\claro\\{output_file_roster}'

query_cmd_roster = (f'sqlcmd -S {claro_server} -d Clarity_PRD_Report '
                    f'-i {Variable.get("claro_query_filepath_roster")} '
                    f'-o {output_path_roster} '
                    f'-s"|" -W -X -I -l 30 -h -1')

copy_cmd_roster = f'pscp -pw {pw} {output_path_roster} {airflow_server_prod}:{basepath}/files'

encrypt_cmd_roster = (f"gpg --encrypt -vv --batch --yes --trust-model always -r "
                      f"claro2020@clarohealthcare.com {basepath}/files/{output_file_roster}")

query_roster = SSHOperator(ssh_conn_id='tableau_server',
                           task_id='query_claro_roster',
                           command=query_cmd_roster,
                           dag=dag)

copy_roster = SSHOperator(ssh_conn_id='tableau_server',
                          task_id='copy_claro_roster',
                          command=copy_cmd_roster,
                          dag=dag)

encrypt_roster = BashOperator(task_id='encrypt_file_roster',
                              bash_command='encrypt_cmd_patient',
                              dag=dag)

sftp_roster = SFTPOperator(task_id='upload_claro_to_sftp_roster',
                           ssh_conn_id='claro_sftp',
                           local_filepath=f'{basepath}/files/{output_file_roster}.gpg',
                           remote_filepath=f'/{output_file_roster}.gpg',
                           create_intermediate_dirs=True,
                           dag=dag)

query_roster >> copy_roster >> encrypt_roster >> sftp_roster
