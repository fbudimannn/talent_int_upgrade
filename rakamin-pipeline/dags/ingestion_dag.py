from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from plugins.pipeline.ingestion_logic import upload_csv, load_to_bigquery


with DAG(
    dag_id="rakamin_ingestion_pipeline",
    start_date=datetime(2023, 1, 1),
    schedule_interval="@daily",
    catchup=False,
):

    t1 = PythonOperator(
        task_id="upload_to_gcs",
        python_callable=upload_csv
    )

    t2 = PythonOperator(
        task_id="load_to_bigquery",
        python_callable=load_to_bigquery,
        op_kwargs={ "files_from_xcom": "{{ ti.xcom_pull(task_ids='upload_to_gcs') }}" }
    )

    t1 >> t2
