from airflow import DAG
from airflow.models import DagRun
from airflow.decorators import task
from airflow.operators.python import PythonOperator


def print_hello_with_date(data_interval_end: DagRun=None):
    print(f"Hello, today is {data_interval_end}")


with DAG(
    dag_id='dynamic_task_with_partial_expand',
    default_args={
        'owner': 'Wonseok Yang',
        'retries': 0
    },
    schedule_interval=None
) as dag:

    task = PythonOperator.partial(
        task_id='dynamic_task',
        python_callable=print_hello_with_date
    ).expand(['test', 'test2', 'test1'])