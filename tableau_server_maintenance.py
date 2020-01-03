from datetime import datetime, timedelta
import pendulum

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable

import pandas as pd
import sqlalchemy as sa
import tableauserverclient as TSC
import requests
import json

from auxiliary.outils import get_json_secret

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

dag = DAG('tableau_server_maintenance', default_args=default_args, catchup=False, schedule_interval='0 1 * * *')

ebi = get_json_secret('ebi_db_conn')['db_connections']['fi_dm_ebi']
pg = get_json_secret('ebi_db_conn')['db_connections']['tableau_pg']

pg_params = '{user}:{password}@{server}:{port}/{database}'.format(**pg)
tpg_engine = sa.create_engine('postgresql+psycopg2://{}'.format(pg_params))
server_url = Variable.get('tableau_server_url')

tableau_auth = TSC.TableauAuth(ebi['user'].split(sep='\\')[1], ebi['password'])
server = TSC.Server(server_url, use_server_version=True)

tagsql = """select s.name as site
      ,p.name as project
      ,w.name as workbook
      ,w.luid as luid
from workbooks w
    inner join sites s
        on w.site_id = s.id
    inner join projects p
        on w.project_id = p.id
            and s.id = p.site_id
    inner join (
                select v.workbook_id
                      ,max(time) as last_viewed_datetime
                    from views_stats vs
                        inner join views v
                            on vs.view_id = v.id
                    where not exists(select 1 from group_users gu where gu.user_id = vs.user_id and gu.group_id = 24)
                    group by v.workbook_id
                ) lv
        on w.id = lv.workbook_id
where s.name = 'EBI'
    and p.name in('Production','Staging')
    and (date_part('day',NOW() - lv.last_viewed_datetime) > 90
        or date_part('day',NOW() - lv.last_viewed_datetime)  is null)
    and not exists(
                    select 1
                        from tags t
                            inner join taggings tg
                                on t.id = tg.tag_id
                            inner join views v
                                on tg.taggable_id = v.id
                        where v.workbook_id = w.id)"""

untagsql = """select s.name as site
      ,p.name as project
      ,w.name as workbook
      ,w.luid as luid
from workbooks w
    inner join sites s
        on w.site_id = s.id
    inner join projects p
        on w.project_id = p.id
            and s.id = p.site_id
    inner join (
                select v.workbook_id
                      ,max(time) as last_viewed_datetime
                    from views_stats vs
                        inner join views v
                            on vs.view_id = v.id
                    where not exists(select 1 from group_users gu where gu.user_id = vs.user_id and gu.group_id = 24)
                    group by v.workbook_id
                ) lv
        on w.id = lv.workbook_id
where s.name = 'EBI'
    and p.name in('Production','Staging')
    and (date_part('day',NOW() - lv.last_viewed_datetime) > 90
        or date_part('day',NOW() - lv.last_viewed_datetime)  is null)
    and not exists(
                    select 1
                        from tags t
                            inner join taggings tg
                                on t.id = tg.tag_id
                            inner join views v
                                on tg.taggable_id = v.id
                        where v.workbook_id = w.id
                            and t.name = 'ArchiveCandidate')"""


def tag():
    df = pd.read_sql(tagsql, tpg_engine)

    server.auth.sign_in(tableau_auth)

    version = str(server.version)
    token = server.auth_token
    header = {'X-Tableau-Auth': token}

    for index, row in df.iterrows():
        id = str(row['luid'])
        site = str(row['site'])
        url = server_url + '/api/' + version + '/sites/' + site + '/workbooks/' + id + '/tags'
        body = """<tsRequest>
                    <tags>
                        <tag label="ArchiveCandidate" />
                    </tags>
                </tsRequest>"""
        requests.put(url, body, headers=header)

    server.auth.sign_out()


def untag():
    df = pd.read_sql(untagsql, tpg_engine)

    server.auth.sign_in(tableau_auth)

    version = str(server.version)
    token = server.auth_token
    header = {'X-Tableau-Auth': token}

    for index, row in df.iterrows():
        id = str(row['luid'])
        site = str(row['site'])
        url = server_url + '/api/' + version + '/sites/' + site + '/workbooks/' + id + '/tags/ArchiveCandidate'
        requests.delete(url, headers=header)

    server.auth.sign_out()


def backup():
    login_url = server_url + ":8850/api/0.5/login"
    logout_url = server_url + ":8850/api/0.5/logout"
    backup_endpoint = server_url + ":8850/api/0.5/backupFixedFile"

    username = ebi['user'].split(sep='\\')[1]
    password = ebi['password']

    headers = {
        'content-type': 'application/json'
        }

    auth = {
        'authentication': {
            'name': username,
            'password': password
            }
        }

    session = requests.Session()
    body = json.dumps(auth)
    now = datetime.now()
    now = now.strftime("%Y%m%dT%H%M%S")
    writep = "backup-" + now + ".tsbak"
    backup_url = backup_endpoint + "?writePath=" + writep + "&jobTimeoutSeconds=7200"

    login_resp = session.post(login_url, data=body, headers=headers, verify=False)
    backup_resp = session.post(backup_url)
    logout_resp = session.post(logout_url)


tg = PythonOperator(
        task_id='Tag_Archive_Candidate',
        python_callable=tag,
        dag=dag
        )

ut = PythonOperator(
        task_id='Untag_Recently_Viewed',
        python_callable=untag,
        dag=dag
        )

bs = PythonOperator(
        task_id='backup_tableau_server',
        python_callable=backup,
        dag=dag
        )

tg >> ut >> bs
