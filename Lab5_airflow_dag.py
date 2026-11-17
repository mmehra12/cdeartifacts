"""
CDE Airflow DAG for Lab 3B.

Runs the two data extraction jobs in parallel, then builds the final report.
"""
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from cloudera.cdp.airflow.operators.cde_operator import CDEJobRunOperator

# Update the username (example: "apac01")
OWNER = "<ENTER YOUR USERNAME HERE>"

if not OWNER or OWNER.startswith("<"):
    raise ValueError("Set OWNER to your CDE username before uploading this DAG.")

DAG_ID = f"{OWNER}_Airflow_Dag"
JOB_SUB_150K = f"{OWNER}_Lab3B1_Data_Extraction_Sub_150k"
JOB_OVER_150K = f"{OWNER}_Lab3B2_Data_Extraction_Over_150k"
JOB_REPORTS = f"{OWNER}_Lab3B3_Create_Reports"

default_args = {
    "owner": OWNER,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "email_on_failure": False,
    "email_on_retry": False,
}

with DAG(
    dag_id=DAG_ID,
    description="Extract loans under and over 150k in parallel, then create reports",
    default_args=default_args,
    start_date=pendulum.datetime(2026, 11, 16, tz="UTC"),
    end_date=pendulum.datetime(2026, 11, 18, tz="UTC"),
    schedule="*/20 * * * *",
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    tags=["cde", "lab3b"],
) as dag:

    start = EmptyOperator(task_id="start")

    extract_sub_150k = CDEJobRunOperator(
        task_id="extract_sub_150k",
        job_name=JOB_SUB_150K,
    )

    extract_over_150k = CDEJobRunOperator(
        task_id="extract_over_150k",
        job_name=JOB_OVER_150K,
    )

    create_reports = CDEJobRunOperator(
        task_id="create_reports",
        job_name=JOB_REPORTS,
    )

    end = EmptyOperator(task_id="end")

    start >> [extract_sub_150k, extract_over_150k] >> create_reports >> end
