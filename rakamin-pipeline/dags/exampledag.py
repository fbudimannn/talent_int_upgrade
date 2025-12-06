from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

# DAG definition
with DAG(
    dag_id="example_simple_dag",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["example", "testing"],
) as dag:

    # Task 1
    task_hello = BashOperator(
        task_id="print_hello",
        bash_command="echo 'Hello from Airflow!'"
    )

    # Task 2
    task_bye = BashOperator(
        task_id="print_goodbye",
        bash_command="echo 'Bye from Airflow!'"
    )

    task_hello >> task_bye
