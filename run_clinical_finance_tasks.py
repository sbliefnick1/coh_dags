from datetime import datetime, timedelta

import pendulum
from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator
from airflow.operators.python import PythonOperator
from auxiliary.outils import refresh_tableau_extract

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

dag = DAG('run_clinical_finance_tasks', default_args=default_args, catchup=False, schedule_interval='35 5 * * *')

conn_id = 'ebi_datamart'
pool_id = 'ebi_etl_pool'

ebi_sql = """
    SELECT
        DATEFROMPARTS(YEAR(L.JOURNAL_DATE), MONTH(L.JOURNAL_DATE), 1) AS MONTH_START,
        CASE
            WHEN L.APPL_JRNL_ID = 'PAYROLL' THEN 'Regular'
            WHEN L.APPL_JRNL_ID = 'PAY_ACCRL' THEN 'Accrual'
        END AS ROLE_TYPE,
        CAST(L.MONETARY_AMOUNT AS numeric(18, 2)) AS DLRS,
        CAST(L.OTH_HRS AS numeric(18, 2)) AS HRS,
        L.DEPTID,
        L.BUSINESS_UNIT_GL,
        L.EMPLID,
        L.ACCOUNT,
        L.ERNCD,
        L.PAY_END_DT,
        E.COH_SEND_TO_HCM,
        L.FUND_CODE,
        L.BUSINESS_UNIT_PC,
        L.PROJECT_ID,
        L.ACTIVITY_ID,
        L.ADJUSTMENT_FLG,
        L.APPL_JRNL_ID,
        L.FISCAL_YEAR,
        L.JOURNAL_DATE,
        L.JRNL_LN_REF,
        L.COMPANY,
        L.PAYGROUP
    INTO #LABOR
    FROM FI_EDW.dbo.PSFT_HR_PS_COH_LABOR L  --select top 100 * from PSFT_HR_PS_COH_LABOR
    INNER JOIN FI_EDW.dbo.PSFT_HR_PS_COH_EARNINGS E
        ON L.ERNCD = E.ERNCD
            AND E.EFFDT = (
                SELECT MAX(E1.EFFDT)
                FROM FI_EDW.dbo.PSFT_HR_PS_EARNINGS_TBL E1
                WHERE E.ERNCD = E1.ERNCD
                    AND E1.EFFDT <= L.PAY_END_DT
            )
    WHERE L.APPL_JRNL_ID IN('PAYROLL', 'PAY_ACCRL')
    
    UNION ALL
    
    SELECT
        DATEADD(month, 1, DATEFROMPARTS(YEAR(L.JOURNAL_DATE), MONTH(L.JOURNAL_DATE), 1)) AS MONTH_START,
        'Reversal' AS ROLE_TYPE,
        CAST(L.MONETARY_AMOUNT AS numeric(18, 2)) * -1 AS DLRS,
        CAST(L.OTH_HRS AS numeric(18, 2)) * -1 AS HRS,
        L.DEPTID,
        L.BUSINESS_UNIT_GL,
        L.EMPLID,
        L.ACCOUNT,
        L.ERNCD,
        L.PAY_END_DT,
        E.COH_SEND_TO_HCM,
        L.FUND_CODE,
        L.BUSINESS_UNIT_PC,
        L.PROJECT_ID,
        L.ACTIVITY_ID,
        L.ADJUSTMENT_FLG,
        L.APPL_JRNL_ID,
        L.FISCAL_YEAR,
        L.JOURNAL_DATE,
        L.JRNL_LN_REF,
        L.COMPANY,
        L.PAYGROUP
    FROM FI_EDW.dbo.PSFT_HR_PS_COH_LABOR L
    INNER JOIN FI_EDW.dbo.PSFT_HR_PS_COH_EARNINGS E
        ON L.ERNCD = E.ERNCD
            AND E.EFFDT = (
                SELECT MAX(E1.EFFDT)
                FROM FI_EDW.dbo.PSFT_HR_PS_EARNINGS_TBL E1
                WHERE E.ERNCD = E1.ERNCD
                    AND E1.EFFDT <= L.PAY_END_DT
            )
        WHERE L.APPL_JRNL_ID = 'PAY_ACCRL';

    SELECT
        BUSINESS_UNIT_GL,
        CASE METRIC
            WHEN 'DLRS' THEN '$'
            WHEN 'HRS' THEN '#'
        END AS REC_TYPE,
        PAY_END_DT,
        ACCOUNT,
        ERNCD,
        ' ' AS OLD_COST_CENTER,  -- THIS IS ALWAYS BLANK IN CURRENT VERSION OF REPORT
        AMT,
        CASE
            WHEN APPL_JRNL_ID = 'PAYROLL' AND ADJUSTMENT_FLG = 'N'
                THEN 0
            WHEN APPL_JRNL_ID = 'PAYROLL' AND ADJUSTMENT_FLG = 'Y'
                THEN 1
            WHEN APPL_JRNL_ID = 'PAY_ACCRL' AND ADJUSTMENT_FLG = 'N' AND ROLE_TYPE = 'Accrual'
                THEN 2
            WHEN APPL_JRNL_ID = 'PAY_ACCRL' AND ADJUSTMENT_FLG = 'N' AND ROLE_TYPE = 'Reversal'
                THEN 4
            WHEN APPL_JRNL_ID = 'PAY_ACCRL' AND ADJUSTMENT_FLG = 'Y' AND ROLE_TYPE = 'Accrual'
                THEN 3
            WHEN APPL_JRNL_ID = 'PAY_ACCRL' AND ADJUSTMENT_FLG = 'Y' AND ROLE_TYPE = 'Reversal'
                THEN 5
        END AS SL_ID,
        EMPLID,
        DEPTID,
        FUND_CODE,
        BUSINESS_UNIT_PC,
        PROJECT_ID,
        ACTIVITY_ID,
        -- ADDED BY JEREMY FOR CONTEXT
        COH_SEND_TO_HCM,
        ADJUSTMENT_FLG,
        ROLE_TYPE,
        APPL_JRNL_ID,
        CASE
            WHEN APPL_JRNL_ID = 'PAYROLL' AND ADJUSTMENT_FLG = 'N'
                THEN 'CURRENT_LABOR'
            WHEN APPL_JRNL_ID = 'PAYROLL' AND ADJUSTMENT_FLG = 'Y'
                THEN 'ADJUSTMENT_LABOR'
            WHEN APPL_JRNL_ID = 'PAY_ACCRL' AND ADJUSTMENT_FLG = 'N' AND ROLE_TYPE = 'Accrual'
                THEN 'CURRENT_ACCRUAL'
            WHEN APPL_JRNL_ID = 'PAY_ACCRL' AND ADJUSTMENT_FLG = 'N' AND ROLE_TYPE = 'Reversal'
                THEN 'CURRENT_REVERSE'
            WHEN APPL_JRNL_ID = 'PAY_ACCRL' AND ADJUSTMENT_FLG = 'Y' AND ROLE_TYPE = 'Accrual'
                THEN 'ADJUSTMENT_ACCRUAL'
            WHEN APPL_JRNL_ID = 'PAY_ACCRL' AND ADJUSTMENT_FLG = 'Y' AND ROLE_TYPE = 'Reversal'
                THEN 'ADJUSTMENT_REVERSE'
        END AS ROLE_ADJ_TYPE,
        FISCAL_YEAR,
        JOURNAL_DATE,
        JRNL_LN_REF,
        COMPANY,
        PAYGROUP,
        MONTH_START
    INTO #BASE
    FROM #LABOR
    UNPIVOT (
        AMT FOR METRIC IN(DLRS, HRS)
    ) UP;

    SELECT
        -- fields form the HCM Labor file
        B.*,
        -- BUSINESS UNIT DETAILS
        BU.DESCR AS BUSINESS_UNIT_DESCR,
        BU.DESCRSHORT AS BUSINESS_UNIT_DESCR_SHORT,
        -- ACCOUNT DETAILS
        A.DESCR AS ACCOUNT_DESCR,
        A.DESCRSHORT AS ACCOUNT_DESCR_SHORT,
        -- PAYCODE DETAILS
        E.DESCR AS ERNCD_DESCR,
        E.DESCRSHORT AS ERNCD_DESCR_SHORT,
        -- DEPT DETAILS
        D.DESCR AS DEPT_DESCR,
        D.DESCRSHORT AS DEPT_DESCR_SHORT,
        -- FUND DETAILS
        F.DESCR AS FUND_DESCR,
        F.DESCRSHORT AS FUND_DESCR_SHORT,
        F.DESCRLONG AS FUND_DESCR_LONG,
        -- PROJECT DETAILS
        P.DESCR AS PROJECT_DESCR,
        P.PROJECT_TYPE,
        -- EMPLOYEE DEMOGRAPHICS
        (
            SELECT
                P.NAME,
                J.EMPL_RCD,
                J.HIRE_DT,
                J.TERMINATION_DT,
                J.EFFDT,
                J.EFFSEQ,
                J.DEPTID,
                J.JOBCODE,
                J.EMPL_STATUS,
                J.COMPANY,
                J.EMPL_TYPE,
                J.STD_HOURS,
                J.GRADE,
                J.STEP,
                J.GL_PAY_TYPE,
                J.ANNUAL_RT,
                J.HOURLY_RT,
                J.BUSINESS_UNIT,
                J.SAL_ADMIN_PLAN,
                J.EMPL_CLASS,
                JC.DESCR AS JOB_TITLE,
                XET.XLATLONGNAME AS EMPL_TYPE_DESCR,
                XEC.XLATLONGNAME AS EMPL_CLASS_DESCR,
                J.FULL_PART_TIME
            FROM FI_EDW.dbo.PSFT_HR_PS_JOB J
            INNER JOIN FI_EDW.dbo.PSFT_HR_PS_PERSONAL_DATA P
                ON J.EMPLID = P.EMPLID
            LEFT JOIN FI_EDW.dbo.PSFT_HR_PS_JOBCODE_TBL JC
                ON J.JOBCODE = JC.JOBCODE
                    AND JC.EFF_STATUS = 'A'
                    AND JC.EFFDT = (
                        SELECT MAX(JCED.EFFDT)
                        FROM FI_EDW.dbo.PSFT_HR_PS_JOBCODE_TBL JCED
                        WHERE JCED.SETID = JC.SETID
                            AND JCED.JOBCODE = JC.JOBCODE
                            AND JCED.EFF_STATUS = JC.EFF_STATUS
                            AND JCED.EFFDT <= J.EFFDT
                    )
            LEFT JOIN FI_EDW.dbo.PSFT_HR_PSXLATITEM XET
                ON J.EMPL_TYPE = XET.FIELDVALUE
                    AND XET.FIELDNAME = 'EMPL_TYPE'
                    AND XET.EFF_STATUS = 'A'
                    AND XET.EFFDT = (
                        SELECT MAX(XETED.EFFDT)
                        FROM FI_EDW.dbo.PSFT_HR_PSXLATITEM XETED
                        WHERE XETED.FIELDNAME = XET.FIELDNAME
                            AND XETED.FIELDVALUE = XET.FIELDVALUE
                            AND XETED.EFF_STATUS = XET.EFF_STATUS
                            AND XETED.EFFDT <= J.EFFDT
                    )
            LEFT JOIN FI_EDW.dbo.PSFT_HR_PSXLATITEM XEC
                ON J.EMPL_CLASS = XEC.FIELDVALUE
                    AND XEC.FIELDNAME = 'EMPL_CLASS'
                    AND XEC.EFF_STATUS = 'A'
                    AND XEC.EFFDT = (
                        SELECT MAX(XECED.EFFDT)
                        FROM FI_EDW.dbo.PSFT_HR_PSXLATITEM XECED
                        WHERE XECED.FIELDNAME = XEC.FIELDNAME
                            AND XECED.FIELDVALUE = XEC.FIELDVALUE
                            AND XECED.EFF_STATUS = XEC.EFF_STATUS
                            AND XECED.EFFDT <= J.EFFDT
                    )
            WHERE J.PER_ORG = 'EMP'
                AND J.ACTION_REASON != 'NWK'
                AND (
                    J.TERMINATION_DT IS NULL
                        OR DATEADD(month, -1, J.TERMINATION_DT) <= B.PAY_END_DT
                )
                AND J.EFFDT = (
                    SELECT MAX(JED.EFFDT)
                    FROM FI_EDW.dbo.PSFT_HR_PS_JOB JED
                    WHERE JED.EMPLID = J.EMPLID
                        AND JED.EMPL_RCD = J.EMPL_RCD
                        AND JED.PER_ORG = 'EMP'
                        AND JED.ACTION_REASON != 'NWK'
                        AND JED.EFFDT <= B.PAY_END_DT
                )
                AND J.EFFSEQ = (
                    SELECT MAX(JES.EFFSEQ)
                    FROM FI_EDW.dbo.PSFT_HR_PS_JOB JES
                    WHERE JES.EMPLID = J.EMPLID
                        AND JES.EMPL_RCD = J.EMPL_RCD
                        AND JES.PER_ORG = 'EMP'
                        AND JES.ACTION_REASON != 'NWK'
                        AND JES.EFFDT = J.EFFDT
                )
                AND J.EMPLID = B.EMPLID
            ORDER BY J.EMPL_RCD DESC
            FOR JSON PATH
        ) AS EMP_DEMO,
        -- FLAGS
        CASE
            WHEN PC.PAY_END_DT IS NULL THEN 'OFF CYCLE PAY'
            ELSE 'ON CYCLE PAY'
        END AS OFF_CYCLE_PAY_FLAG
    INTO #COMBINED
    FROM #BASE B
    LEFT JOIN FI_EDW.dbo.PSFT_PS_BUS_UNIT_TBL_FS BU
        ON B.BUSINESS_UNIT_GL = BU.BUSINESS_UNIT
    LEFT JOIN FI_EDW.dbo.PSFT_PS_GL_ACCOUNT_TBL A
        ON B.ACCOUNT = A.ACCOUNT
            AND A.EFF_STATUS = 'A'
            AND A.EFFDT = (
                SELECT MAX(AED.EFFDT)
                FROM FI_EDW.dbo.PSFT_PS_GL_ACCOUNT_TBL AED
                WHERE AED.ACCOUNT = A.ACCOUNT
                    AND AED.EFF_STATUS = A.EFF_STATUS
                    AND AED.SETID = A.SETID
                    AND AED.EFFDT <= B.PAY_END_DT
            )
    INNER JOIN FI_EDW.dbo.PSFT_HR_PS_EARNINGS_TBL E
        ON B.ERNCD = E.ERNCD
            AND E.EFF_STATUS = 'A'
            AND E.EFFDT = (
                SELECT MAX(EED.EFFDT)
                FROM FI_EDW.dbo.PSFT_HR_PS_EARNINGS_TBL EED
                WHERE EED.ERNCD = E.ERNCD
                    AND EED.EFF_STATUS = E.EFF_STATUS
                    AND EED.EFFDT <= B.PAY_END_DT
            )
    INNER JOIN FI_EDW.dbo.PSFT_HR_PS_DEPT_TBL D
        ON B.DEPTID = D.DEPTID
            AND D.EFF_STATUS = 'A'
            AND D.EFFDT = (
                SELECT MAX(DED.EFFDT)
                FROM FI_EDW.dbo.PSFT_HR_PS_DEPT_TBL DED
                WHERE DED.DEPTID = D.DEPTID
                    AND DED.EFF_STATUS = D.EFF_STATUS
                    AND DED.EFFDT <= B.PAY_END_DT
            )
    LEFT JOIN FI_EDW.dbo.PSFT_PS_FUND_TBL F
        ON B.FUND_CODE = F.FUND_CODE
            AND F.EFF_STATUS = 'A'
            AND F.EFFDT = (
                SELECT MAX(FED.EFFDT)
                FROM FI_EDW.dbo.PSFT_PS_FUND_TBL FED
                WHERE FED.FUND_CODE = F.FUND_CODE
                    AND FED.EFF_STATUS = F.EFF_STATUS
                    AND FED.EFFDT <= B.PAY_END_DT
            )
    LEFT JOIN FI_EDW.dbo.PSFT_PS_PROJECT P
        ON B.BUSINESS_UNIT_PC = P.BUSINESS_UNIT
            AND B.PROJECT_ID = P.PROJECT_ID
            AND P.EFF_STATUS = 'A'
    LEFT JOIN (
        SELECT
            COMPANY,
            PAYGROUP,
            PAY_END_DT
        FROM FI_EDW.dbo.PSFT_HR_PS_PAY_CALENDAR
        WHERE PAY_OFF_CYCLE_CAL = 'N'
            AND RUN_ID != ' '
    ) PC
        ON B.COMPANY = PC.COMPANY
            AND B.PAYGROUP = PC.PAYGROUP
            AND B.PAY_END_DT = PC.PAY_END_DT;

    DROP TABLE IF EXISTS FI_DM_EBI.dbo.EBI_Enterprise_Labor;

    SELECT
        MONTH_START AS REPORTING_MONTH,
        DATEFROMPARTS(YEAR(JOURNAL_DATE), MONTH(JOURNAL_DATE), 1) AS JOURNAL_MONTH,
        JOURNAL_DATE,
        DATEFROMPARTS(YEAR(PAY_END_DT), MONTH(PAY_END_DT), 1) AS PAY_END_MONTH,
        BUSINESS_UNIT_GL,
        REC_TYPE,
        PAY_END_DT,
        ACCOUNT,
        ERNCD,
        CASE
            WHEN ERNCD IN ('OVT', 'DBL') THEN 1
            ELSE 0 
        END AS OVERTIME_FLG,
        OLD_COST_CENTER,
        AMT,
        SL_ID,
        EMPLID,
        DEPTID,
        FUND_CODE,
        BUSINESS_UNIT_PC,
        PROJECT_ID,
        ACTIVITY_ID,
        COH_SEND_TO_HCM,
        ADJUSTMENT_FLG,
        ROLE_TYPE,
        APPL_JRNL_ID,
        ROLE_ADJ_TYPE,
        FISCAL_YEAR,
        JRNL_LN_REF,
        COMPANY,
        PAYGROUP,
        BUSINESS_UNIT_DESCR,
        BUSINESS_UNIT_DESCR_SHORT,
        ACCOUNT_DESCR,
        ACCOUNT_DESCR_SHORT,
        ERNCD_DESCR,
        ERNCD_DESCR_SHORT,
        DEPT_DESCR,
        DEPT_DESCR_SHORT,
        FUND_DESCR,
        FUND_DESCR_SHORT,
        FUND_DESCR_LONG,
        PROJECT_DESCR,
        PROJECT_TYPE,
        OFF_CYCLE_PAY_FLAG,
        JSON_VALUE(EMP_DEMO, '$[0].NAME') AS EMPLOYEE_NAME,
        CAST(JSON_VALUE(EMP_DEMO, '$[0].EMPL_RCD') AS int) AS EMPL_RCD,
        CAST(JSON_VALUE(EMP_DEMO, '$[0].HIRE_DT') AS date) AS HIRE_DT,
        CAST(JSON_VALUE(EMP_DEMO, '$[0].EFFDT') AS date) AS EFFDT,
        CAST(JSON_VALUE(EMP_DEMO, '$[0].EFFSEQ') AS int) AS EFFSEQ,
        JSON_VALUE(EMP_DEMO, '$[0].DEPTID') AS HOME_DEPTID,
        JSON_VALUE(EMP_DEMO, '$[0].JOBCODE') AS JOBCODE,
        JSON_VALUE(EMP_DEMO, '$[0].EMPL_STATUS') AS EMPL_STATUS,
        JSON_VALUE(EMP_DEMO, '$[0].COMPANY') AS HOME_COMPANY,
        JSON_VALUE(EMP_DEMO, '$[0].EMPL_TYPE') AS EMPL_TYPE,
        CAST(JSON_VALUE(EMP_DEMO, '$[0].STD_HOURS') AS numeric(18, 2)) AS STD_HOURS,
        JSON_VALUE(EMP_DEMO, '$[0].GRADE') AS GRADE,
        JSON_VALUE(EMP_DEMO, '$[0].STEP') AS STEP,
        JSON_VALUE(EMP_DEMO, '$[0].GL_PAY_TYPE') AS GL_PAY_TYPE,
        CAST(JSON_VALUE(EMP_DEMO, '$[0].ANNUAL_RT') AS numeric(18, 2)) AS ANNUAL_RT,
        CAST(JSON_VALUE(EMP_DEMO, '$[0].HOURLY_RT') AS numeric(18, 2)) AS HOURLY_RT,
        JSON_VALUE(EMP_DEMO, '$[0].BUSINESS_UNIT') AS BUSINESS_UNIT,
        JSON_VALUE(EMP_DEMO, '$[0].SAL_ADMIN_PLAN') AS SAL_ADMIN_PLAN,
        JSON_VALUE(EMP_DEMO, '$[0].EMPL_CLASS') AS EMPL_CLASS,
        JSON_VALUE(EMP_DEMO, '$[0].JOB_TITLE') AS JOB_TITLE,
        JSON_VALUE(EMP_DEMO, '$[0].EMPL_TYPE_DESCR') AS EMPL_TYPE_DESCR,
        JSON_VALUE(EMP_DEMO, '$[0].EMPL_CLASS_DESCR') AS EMPL_CLASS_DESCR,
        JSON_VALUE(EMP_DEMO, '$[0].FULL_PART_TIME') AS FULL_PART_TIME
    INTO FI_DM_EBI.dbo.EBI_Enterprise_Labor
    FROM #COMBINED;
"""

cfin_sql = """
    DROP TABLE IF EXISTS FI_DM_CLINICAL_FINANCE.dbo.EBI_Enterprise_Labor;

    SELECT
        REPORTING_MONTH,
        JOURNAL_MONTH,
        JOURNAL_DATE,
        PAY_END_MONTH,
        BUSINESS_UNIT_GL,
        REC_TYPE,
        PAY_END_DT,
        ACCOUNT,
        ERNCD,
        OVERTIME_FLG,
        OLD_COST_CENTER,
        AMT,
        SL_ID,
        EMPLID,
        DEPTID,
        FUND_CODE,
        BUSINESS_UNIT_PC,
        PROJECT_ID,
        ACTIVITY_ID,
        COH_SEND_TO_HCM,
        ADJUSTMENT_FLG,
        ROLE_TYPE,
        APPL_JRNL_ID,
        ROLE_ADJ_TYPE,
        FISCAL_YEAR,
        JRNL_LN_REF,
        COMPANY,
        PAYGROUP,
        BUSINESS_UNIT_DESCR,
        BUSINESS_UNIT_DESCR_SHORT,
        ACCOUNT_DESCR,
        ACCOUNT_DESCR_SHORT,
        ERNCD_DESCR,
        ERNCD_DESCR_SHORT,
        DEPT_DESCR,
        DEPT_DESCR_SHORT,
        FUND_DESCR,
        FUND_DESCR_SHORT,
        FUND_DESCR_LONG,
        PROJECT_DESCR,
        PROJECT_TYPE,
        OFF_CYCLE_PAY_FLAG,
        EMPLOYEE_NAME,
        EMPL_RCD,
        HIRE_DT,
        EFFDT,
        EFFSEQ,
        HOME_DEPTID,
        JOBCODE,
        EMPL_STATUS,
        HOME_COMPANY,
        EMPL_TYPE,
        STD_HOURS,
        GRADE,
        STEP,
        GL_PAY_TYPE,
        ANNUAL_RT,
        HOURLY_RT,
        BUSINESS_UNIT,
        SAL_ADMIN_PLAN,
        EMPL_CLASS,
        JOB_TITLE,
        EMPL_TYPE_DESCR,
        EMPL_CLASS_DESCR,
        FULL_PART_TIME
    INTO FI_DM_CLINICAL_FINANCE.dbo.EBI_Enterprise_Labor
    FROM FI_DM_EBI.dbo.EBI_Enterprise_Labor;
"""

refresh_maps_bash = 'cd C:\\Anaconda\\ETL\\clinical_finance && python cfin_maps_to_ebi.py'

m = SSHOperator(
    ssh_conn_id='tableau_server',
    task_id='refresh_mapping_tables',
    command=refresh_maps_bash,
    dag=dag
)

ebi = MsSqlOperator(
    sql=ebi_sql,
    task_id='refresh_labor_table_in_ebi',
    autocommit=True,
    mssql_conn_id=conn_id,
    pool=pool_id,
    dag=dag
)

cfin = MsSqlOperator(
    sql=cfin_sql,
    task_id='refresh_labor_table_in_clinical_finance',
    autocommit=True,
    mssql_conn_id=conn_id,
    pool=pool_id,
    dag=dag
)

tab = PythonOperator(
    task_id='refresh_labor_table_in_tableau',
    python_callable=refresh_tableau_extract,
    op_kwargs={'datasource_id': '149fbbfa-b146-454e-be88-f7c365ccafbe'},
    dag=dag
)

ebi >> tab
ebi >> cfin
