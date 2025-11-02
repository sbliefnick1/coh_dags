from datetime import datetime, timedelta

import pendulum
from airflow import DAG
from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 12, 12, tzinfo=pendulum.timezone('America/Los_Angeles')),
    'email': ['jharris@coh.org'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

with DAG('run_master_etl', default_args=default_args, catchup=False, schedule_interval='45 5 * * *') as dag:
    conn_id = 'ebi_datamart'
    pool_id = 'ebi_etl_pool'

    sql_ops = {}

    procs = [
        'ebi_bridge_patient_problem_list_logic',
        'ebi_dim_bed_clarity_logic',
        'ebi_dim_care_provider_clarity_logic',
        'ebi_dim_change_commitments_open_processes_ebuilder_logic',
        'ebi_dim_coh_stat_tickets_logic',
        'ebi_dim_cpt_logic',
        'ebi_dim_department_clarity_logic',
        'ebi_dim_employee_logic',
        'ebi_dim_hospital_account_clarity_logic',
        'ebi_dim_icd_clarity_logic',
        'ebi_dim_office_of_educational_technology_genius_logic',
        'ebi_dim_order_clarity_logic',
        'ebi_dim_patient_clarity_logic',
        'ebi_dim_patient_satisfaction_survey_logic',
        'ebi_dim_payor_plan_clarity_logic',
        'ebi_dim_portfolio_project_logic',
        'ebi_dim_projects_pms_budgets_ebuilder_logic',
        'ebi_dim_research_study_clarity_logic',
        'ebi_dim_research_study_subject_logic',
        'ebi_dim_retail_food_sales_nextep_logic',
        'ebi_dim_room_clarity_logic',
        'ebi_dim_transport_logic',
        'ebi_dim_workrequests_archibus_logic',
        'ebi_fact_change_commitments_open_processes_ebuilder_logic',
        'ebi_fact_coh_stat_tickets_logic',
        'ebi_fact_office_of_educational_technology_genius_logic',
        'ebi_fact_patient_clarity_logic',
        'ebi_fact_patient_satisfaction_comment_clarity_logic',
        'ebi_fact_portfolio_project_logic',
        'ebi_fact_portfolio_project_time_record_logic',
        'ebi_fact_projects_pms_budgets_ebuilder_logic',
        'ebi_fact_research_study_subject_logic',
        'ebi_fact_retail_food_sales_nextep_logic',
        'ebi_fact_workrequests_archibus_logic'
    ]

    for p in procs:
        o = MsSqlOperator(
            sql='exec {};'.format(p),
            task_id='exec_{}'.format(p),
            autocommit=True,
            mssql_conn_id=conn_id,
            pool=pool_id,
            dag=dag
        )
        sql_ops[p] = o

exec_ebi_dim_cpt_logic >> exec_ebi_fact_patient_clarity_logic
