from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'cloud_engineering',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='alpha_financial_mwaa_orchestration',
    default_args=default_args,
    description='Managed Cloud Transformation and Cloud ML Inference Loop',
    schedule=timedelta(minutes=5),  # FIXED: Changed from schedule_interval to schedule
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['production', 'mwaa_stack'],
) as dag:

    # Task 1: Open secure tunnel to EC2 and execute dbt transformations
    run_dbt_transformations = SSHOperator(
        task_id='run_dbt_transformations',
        ssh_conn_id='ec2_compute_node',
        command='cd ~/Streaming-Financial-Analytics-Platform/dbt_analytics && ../venv/bin/dbt run'
    )

    # Task 2: Open secure tunnel to EC2 and trigger Hugging Face ML inference loop
    execute_ml_inference = SSHOperator(
        task_id='execute_ml_inference',
        ssh_conn_id='ec2_compute_node',
        command='cd ~/Streaming-Financial-Analytics-Platform && ./venv/bin/python3 ml_inference_engine.py'
    )

    # Establish the sequential workflow dependency line
    run_dbt_transformations >> execute_ml_inference