from pathlib import Path
from datetime import datetime, timedelta
import logging
import re
from urllib.parse import quote_plus

import pandas as pd
import pendulum
import sqlalchemy as sa
from tableaudocumentapi import Datasource
import tableauserverclient as TSC

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.sensors import ExternalTaskSensor

from auxiliary.outils import get_json_secret

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2018, 10, 2, tzinfo=pendulum.timezone('America/Los_Angeles')),
    'email': ['sbliefnick@coh.org', 'jharris@coh.org'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
    }

dag = DAG('get_etl_deps', default_args=default_args, catchup=False, schedule_interval='35 5 * * *')

ebi = get_json_secret('ebi_db_conn')['db_connections']['fi_dm_ebi']
pg = get_json_secret('ebi_db_conn')['db_connections']['tableau_pg']

ppw_params = quote_plus('DRIVER={driver};'
                        'SERVER={server};'
                        'DATABASE={database};'
                        'UID={user};'
                        'PWD={password};'
                        'PORT={port};'
                        'TDS_Version={tds_version};'
                        .format(**ebi))

pg_params = '{user}:{password}@{server}:{port}/{database}'.format(**pg)

ppw_engine = sa.create_engine('mssql+pyodbc:///?odbc_connect={}'.format(ppw_params))
tpg_engine = sa.create_engine('postgresql+psycopg2://{}'.format(pg_params))

auth = TSC.TableauAuth(ebi['user'].split(sep='\\')[1], ebi['password'])
server = TSC.Server('https://ebi.coh.org', use_server_version=True)

ds_folder = Path('/var/nfsshare/datasources/')


def drop_improperly_named(dataframe, column, name_type):
    bad_values = []
    for n in dataframe[column].unique():
        # check that the names don't have disallowed characters, but if they do
        if not re.match(r'^[A-Za-z0-9_\-.]+$', n):
            message = ('The {} name ({}) must be alphanumeric characters,'
                       ' dashes, dots, and underscores exclusively'
                       .format(name_type, n))
            # log it
            logging.warning(message)
            bad_values.append(n)

    # remove rows with those values before continuing
    return dataframe[~dataframe[column].isin(bad_values)]


def query_and_save(db_engine):
    # get procedure-view relationship
    sql = 'select * from vw_ebi_airflow_etl_diagram'
    df = pd.read_sql(sql, db_engine)

    # remove slashes
    df['ds_name'] = df['ds_name'].str.replace('/', '')
    df['proc_name'] = df['proc_name'].str.replace('/', '')

    df = drop_improperly_named(df, 'ds_name', 'data source')
    df = drop_improperly_named(df, 'proc_name', 'procedure')

    # get unique procs by combining procs and dependencies + '_logic'
    u_procs = df.proc_name.unique().tolist()
    deps = df.dependency_name[df.dependency_name != ''] + '_logic'
    u_deps = deps.unique().tolist()
    uniques = list(set(u_procs + u_deps))
    unique_procs = pd.DataFrame(uniques, columns=['procs'])

    # get tables that have no dependency
    no_dep_procs = pd.DataFrame(df[(df.dependency_name == '') & (df.num_dependencies == 0)].proc_name.unique(),
                                columns=['proc_name'])

    # get tables that have at least one dependency
    dep_procs = df[(df.dependency_name != '') & (df.num_dependencies != 0)][['proc_name',
                                                                             'dependency_name']].drop_duplicates()
    # create grouping of procs with tables they rely on
    proc_map = (dep_procs.groupby('proc_name')['dependency_name']
                .apply(list)
                .to_frame()
                .reset_index())

    # create df of procs that have dependencies to loop over and set downstream
    unique_dep_procs = pd.DataFrame(dep_procs.proc_name.unique().tolist(), columns=['procs'])

    # get unique data sources
    unique_ds = df[['id', 'ds_name']].drop_duplicates().reset_index(drop=True)

    # create df of ds procs that have dependencies to loop over and set downstream
    unique_ds_procs = df.loc[df.dependency_name != '', ['id', 'ds_name', 'proc_name']].drop_duplicates().reset_index(
            drop=True)

    # create grouping of data sources with procs they rely on
    ds_map = pd.DataFrame()

    for ds in unique_ds.ds_name:
        # get non-null dependencies of procs
        # if there are dependent procs but no dependencies of those procs.
        # create an empty set
        try:
            dep_set = (df[(df['ds_name'] == ds) & (df['dependency_name'] != '')]
                .groupby(['id', 'ds_name'])['dependency_name']
                .apply(set)
                .values[0])
            dep_set = {d + '_logic' for d in dep_set}
        except IndexError:
            dep_set = set()

        # get procs
        proc_set = (df[df['ds_name'] == ds]
            .groupby(['id', 'ds_name'])['proc_name']
            .apply(set)
            .values[0])

        # determine which dependencies are already covered by procs,
        # i.e., which procs will already be downstream from the dependencies
        # so we don't need to set the refreshes downstream from the same deps
        to_keep = proc_set - dep_set
        for p in to_keep:
            temp = df[(df['ds_name'] == ds) & (df['proc_name'] == p)]
            ds_map = pd.concat([ds_map, temp], ignore_index=True)

    ds_map = ds_map[['id', 'ds_name', 'proc_name']].drop_duplicates().reset_index(drop=True)
    ds_map = (ds_map.groupby(['id', 'ds_name'])['proc_name']
              .apply(list)
              .to_frame()
              .reset_index())

    dfs = {'unique_procs': unique_procs,
           'no_dep_procs': no_dep_procs,
           'proc_map': proc_map,
           'unique_dep_procs': unique_dep_procs,
           'unique_ds': unique_ds,
           'ds_map': ds_map,
           'unique_ds_procs': unique_ds_procs}

    for name, df in dfs.items():
        df.to_json('/var/nfsshare/etl_deps/dev_{}.json'.format(name))


def get_datasources(tableau_server, tableau_authentication):
    tableau_server.auth.sign_in(tableau_authentication)
    df = pd.DataFrame()

    for ds in TSC.Pager(tableau_server.datasources):
        ls = [[ds.id, ds.name, ds.certified, ds.project_name, ds.datasource_type]]
        df = df.append(pd.DataFrame(ls, columns=['id', 'name', 'cert', 'project', 'ds_type']), ignore_index=True)

    tableau_server.auth.sign_out()
    df = df[(df.ds_type == 'sqlserver') & (df.project == 'Datasource')]

    ds_to_remove = [
        # tds that point to procs, not views
        'New E&M Provider Clarity',
        'Patient Trajectory Clarity',
        'CFIM Charges',
        # embedded joins or custom sql; remove or recreate to use a view
        'EBI_Call_Light (FI_DM_EBI)',
        'Via Onc + Charges- Patient Level',
        'Via Onc + Charges- Visit Level'
        ]

    for ds in ds_to_remove:
        df = df[df.name != ds]

    df = df.reset_index().drop(['index'], axis=1)
    return df.to_json()


def join_postgres_data(postgres_engine, **context):
    dataframe = (pd.read_json(context['task_instance'].xcom_pull(task_ids='get_datasources'))
                 .reset_index()
                 .drop(['index'], axis=1))
    sql = """
    select cast(luid as varchar) as id
    , extracts_refreshed_at
    , id as postgres_id
    from datasources 
    where site_id = 1
    """
    pg_df = pd.read_sql(sql, postgres_engine)
    return dataframe.merge(pg_df, how='left', on='id').to_json()


def download_datasources(tableau_server, tableau_authentication, directory, **context):
    datasource_dataframe = (pd.read_json(context['task_instance'].xcom_pull(task_ids='join_postgres_data'))
                            .reset_index()
                            .drop(['index'], axis=1))
    df = pd.DataFrame()

    tableau_server.auth.sign_in(tableau_authentication)
    for i in range(len(datasource_dataframe)):
        ds_id = datasource_dataframe['id'][i]
        filename = directory / (ds_id + '.tdsx')

        tableau_server.datasources.download(ds_id, filepath=filename.as_posix(), include_extract=False)
        tds = Datasource.from_file(filename=filename.as_posix())
        ds_xml = tds._datasourceXML

        df_i = datasource_dataframe.iloc[[i]]

        sql_object = (ds_xml.find('connection').find('relation').get('table'))

        if not sql_object:
            sql_object = (ds_xml.find('connection')
                          .find('relation')
                          .get('stored-proc'))

        df_i = df_i.assign(sql_object_name=sql_object
                           .replace('[dbo].[', '')
                           .replace(']', ''))
        df_i = df_i.assign(server=tds.connections[0].server)
        df_i = df_i.assign(db=tds.connections[0].dbname)

        df = df.append(df_i)

    tableau_server.auth.sign_out()

    df['db'] = df['db'].str.upper()

    df = df[df.db == 'FI_DM_EBI'].reset_index().drop(['index'], axis=1)
    return df.to_json()


def join_ebi_data(mssql_engine, **context):
    dataframe = (pd.read_json(context['task_instance'].xcom_pull(task_ids='download_datasources'))
                 .reset_index()
                 .drop(['index'], axis=1))
    sql = """
    select object_id as sql_object_id
    , name as sql_object_name 
    from sys.views 
    where left(name, 7) = 'vw_EBI_'
    """
    mssql_df = pd.read_sql(sql, mssql_engine)
    df = dataframe.merge(mssql_df, how='left')
    df.to_sql('EBI_View_Datasource_Crosswalk', mssql_engine, if_exists='replace', index=False)


#connect = ExternalTaskSensor(external_dag_id='probe_db_access',
#                             external_task_id='attempt_to_connect',
#                             task_id='wait_for_access',
#                             dag=dag)

query_and_save_deps = PythonOperator(task_id='query_and_save_deps',
                                     python_callable=query_and_save,
                                     op_kwargs={'db_engine': ppw_engine},
                                     dag=dag)

#get_ds = PythonOperator(task_id='get_datasources',
#                        python_callable=get_datasources,
#                        op_kwargs={'tableau_server': server,
#                                   'tableau_authentication': auth},
#                        dag=dag)

#join_pg_data = PythonOperator(task_id='join_postgres_data',
#                              python_callable=join_postgres_data,
#                              provide_context=True,
#                              op_kwargs={'postgres_engine': tpg_engine},
#                              dag=dag)

#download_ds = PythonOperator(task_id='download_datasources',
#                             python_callable=download_datasources,
#                             provide_context=True,
#                             op_kwargs={'tableau_server': server,
#                                        'tableau_authentication': auth,
#                                        'directory': ds_folder},
#                             dag=dag)

#join_data = PythonOperator(task_id='join_ebi_data',
#                           python_callable=join_ebi_data,
#                           provide_context=True,
#                           op_kwargs={'mssql_engine': ppw_engine},
#                           dag=dag)

#connect >> get_ds >> join_pg_data >> download_ds >> join_data >> query_and_save_deps
#connect >> 
query_and_save_deps