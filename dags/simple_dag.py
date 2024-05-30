from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'simple_dag',
    default_args=default_args,
    description='A simple tutorial DAG',
    schedule=timedelta(days=1),
)

start = EmptyOperator(
    task_id='start',
    dag=dag,
)

run_bash = BashOperator(
    task_id='run_bash',
    bash_command='echo "Hello, World!"',
    dag=dag,
)

start >> run_bash
