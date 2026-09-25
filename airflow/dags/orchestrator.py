import os
import time
from datetime import datetime, timezone
from airflow.sdk import dag, task
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import RunLifeCycleState, RunResultState

@dag(
        dag_id="orchestrator",
        schedule="0 7 1 * *", # 12:30 PM IST on the first day of every month
        catchup=False,
        start_date=datetime(2026, 9, 1, tzinfo=timezone.utc),
)
def orchestrator():
    @task()
    def ingest_cdc():
        host = os.getenv("DATABRICKS_HOST")
        token = os.getenv("DATABRICKS_TOKEN")

        if not host or not token:
            raise ValueError(
                "Missing DATABRICKS_HOST or DATABRICKS_TOKEN. "
                "Set them in the environment before running Airflow."
            )

        ws = WorkspaceClient(
            host=host,
            token=token
        )

        jobId = os.getenv("DATABRICKS_JOBID")
        if not jobId:
            raise ValueError(
                "Missing DATABRICKS_JOBID. "
                "Set it in the environment before running Airflow."
            )
        
        job_trigger = ws.jobs.run_now(job_id=jobId)

        while True:

            job_run = ws.jobs.get_run(job_trigger.run_id)

            if job_run.state.life_cycle_state in [RunLifeCycleState.TERMINATED, RunLifeCycleState.SKIPPED, RunLifeCycleState.INTERNAL_ERROR]:
                if job_run.state.result_state == RunResultState.SUCCESS:
                    print("Job completed successfully!")
                    break 
                else:
                    raise Exception(f"Job failed with state: {job_run.state.result_state}")
                    
            time.sleep(5)  # Wait for 5 seconds before checking the job status again
        
        return "CDC Ingestion Completed"
    
    @task.bash
    def clean_target():
        return "rm -rf /opt/airflow/data_engineering_project/target && rm -rf /opt/airflow/data_engineering_project/logs"

    @task.bash
    def source_freshness():
        # Manually set the working directory using the 'cd' command before running the dbt command
        return "cd /opt/airflow/data_engineering_project && dbt source freshness"

    # silver_technical = BashOperator(
    #     task_id='silver_technical',
    #     bash_command='cd /opt/airflow/data_engineering_project && dbt run --select silver_tech',
    # )
    @task.bash
    def silver_technical():
        return "cd /opt/airflow/data_engineering_project && dbt run --select silver_tech"
    
    @task.bash
    def silver_technical_tests():
        return "cd /opt/airflow/data_engineering_project && dbt test --select silver_tech"
    
    @task.bash
    def silver_business():
        return "cd /opt/airflow/data_engineering_project && dbt run --select silver_business"
    
    @task.bash
    def silver_business_tests():
        return "cd /opt/airflow/data_engineering_project && dbt test --select silver_business"
    
    @task.bash
    def gold_ephemeral():
        return "cd /opt/airflow/data_engineering_project && dbt run --select gold/ephemeral"
    
    @task.bash
    def gold_dimensions():
        return "cd /opt/airflow/data_engineering_project && dbt snapshot"
    
    @task.bash
    def gold_fact():
        return "cd /opt/airflow/data_engineering_project && dbt run --select gold/fact"

    
    ingest_cdc() >> clean_target() >> source_freshness() >> silver_technical() >> silver_technical_tests() >> silver_business() >> silver_business_tests() >> gold_ephemeral() >> gold_dimensions() >> gold_fact()


orchestrator_dag = orchestrator()