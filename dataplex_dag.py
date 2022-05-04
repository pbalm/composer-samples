import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import timedelta

default_args = {
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': airflow.utils.dates.days_ago(0)
}

dag = DAG(
    'liveness',
    default_args=default_args,
    description='liveness monitoring dag',
    schedule_interval=timedelta(minutes=1))

t1 = BashOperator(
    task_id='echo', bash_command='echo test', dag=dag, depends_on_past=False)