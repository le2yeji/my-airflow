from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 2, 13),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('simple_bash_operator_example',
          default_args=default_args,
          description='A simple DAG with a BashOperator',
          schedule_interval=timedelta(days=1))

# BashOperator를 사용하여 간단한 출력을 수행합니다.
task_print_hello = BashOperator(
    task_id='print_hello',
    bash_command='echo "Hello, Airflow!"',
    dag=dag,
)

# BashOperator를 사용하여 5초 동안 대기합니다.
task_sleep = BashOperator(
    task_id='sleep_for_5_seconds',
    bash_command='sleep 5',
    dag=dag,
)

# task_print_hello가 task_sleep에 성공한 후에 실행됩니다.
task_print_hello >> task_sleep
