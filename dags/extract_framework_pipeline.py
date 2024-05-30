from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.task_group import TaskGroup

from framework.framework import extract_framework_info, enrich_with_companies, extract_mappings, final_step

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'extract_framework_pipeline',
    default_args=default_args,
    description='A simple release processing pipeline',
    schedule_interval=None,  # Manually triggered
    start_date=datetime(2023, 1, 1),
    catchup=False,
)

release_id = '{{ dag_run.conf["release_id"] if dag_run else "default_release_id" }}'

with TaskGroup("process_release") as process_release:
    extract_task = PythonOperator(
        task_id='extract_framework_info',
        python_callable=extract_framework_info,
        op_kwargs={'release_id': release_id},
        dag=dag,
    )

    enrich_task = PythonOperator(
        task_id='enrich_with_companies',
        python_callable=enrich_with_companies,
        op_kwargs={'release_id': release_id},
        dag=dag,
    )

    mapping_task = PythonOperator(
        task_id='extract_mappings',
        python_callable=extract_mappings,
        op_kwargs={'release_id': release_id},
        dag=dag,
    )

    extract_task >> enrich_task >> mapping_task

final_task = PythonOperator(
    task_id='final_step',
    python_callable=final_step,
    op_kwargs={'release_id': release_id},
    dag=dag,
)

process_release >> final_task
