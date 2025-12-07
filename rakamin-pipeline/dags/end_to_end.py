from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
from plugins.pipeline.ingestion_logic import upload_csv, load_to_bigquery

# Path folder dbt di dalam Airflow container
DBT_DIR = "/usr/local/airflow/rakamin_dbt"


with DAG(
    dag_id="full_end_to_end_pipeline",
    start_date=datetime(2023, 1, 1),
    schedule_interval="@daily",
    catchup=False,
):

    # -------------------------------------------------------------
    # 1. Upload CSV → GCS
    # -------------------------------------------------------------
    t_upload = PythonOperator(
        task_id="upload_to_gcs",
        python_callable=upload_csv,
    )

    # -------------------------------------------------------------
    # 2. Load CSV → BigQuery
    # -------------------------------------------------------------
    t_raw_bq = PythonOperator(
        task_id="load_to_bigquery",
        python_callable=load_to_bigquery,
        op_args=["{{ ti.xcom_pull(task_ids='upload_to_gcs') }}"],
    )

    # -------------------------------------------------------------
    # 3. DBT STAGING MODELS
    # -------------------------------------------------------------
    t_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command=f"""
            cd {DBT_DIR} &&
            dbt run --select staging
        """
    )

    # -------------------------------------------------------------
    # 4. DBT INTERMEDIATE MODELS
    # -------------------------------------------------------------
    t_intermediate = BashOperator(
        task_id="dbt_run_intermediate",
        bash_command=f"""
            cd {DBT_DIR} &&
            dbt run --select intermediate
        """
    )


#Misal mau per dbt tinggal selectnya diedit
#     t_stg_employees = BashOperator(
#     task_id="dbt_stg_employees",
#     bash_command=f"""
#         cd {DBT_DIR} &&
#         dbt run --select stg_employees
#     """
# )

    # # -------------------------------------------------------------
    # # 5. DBT MARTS (kalau punya)
    # # -------------------------------------------------------------
    # t_marts = BashOperator(
    #     task_id="dbt_run_marts",
    #     bash_command=f"""
    #         cd {DBT_DIR} &&
    #         dbt run --select marts
    #     """
    # )

    # -------------------------------------------------------------
    # DEPENDENCIES (FLOW)
    # -------------------------------------------------------------
    t_upload >> t_raw_bq >> t_staging >> t_intermediate 
