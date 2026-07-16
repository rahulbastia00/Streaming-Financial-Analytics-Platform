from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'sanity_check',
    'start_date': datetime(2026, 1, 1),
}

with DAG(
    'aaa_mwaa_sanity_check',
    default_args=default_args,
    schedule=None,  # Manual trigger only
    catchup=False
) as dag:

    test_task = BashOperator(
        task_id='say_hello',
        bash_command='echo "MWAA network storage link is perfectly active!"'
    )