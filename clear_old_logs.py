from datetime import datetime, timedelta

import pendulum

from airflow import DAG
from airflow.operators.bash_operator import BashOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 10, 5, tzinfo=pendulum.timezone('America/Los_Angeles')),
    'email': ['sbliefnick@coh.org'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=3)
    }

dag = DAG('clear_old_logs', default_args=default_args, catchup=False, schedule_interval='0 1 * * *')

clear_cmd = "sudo find /var/nfsshare/logs -mtime +90 | sudo xargs rm -rf; df -h"

clear = BashOperator(task_id='clear_old_logs',
                     bash_command=clear_cmd,
                     dag=dag)
