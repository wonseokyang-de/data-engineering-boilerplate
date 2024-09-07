from airflow import DAG
from airflow.models import DagRun
from airflow.decorators import task


with DAG(
    dag_id='taskflowapi',
    default_args={
        'owner': 'Wonseok Yang',
        'retries': 0
    },
    schedule_interval=None
) as dag:

    @task
    def example_task(data_interval_start: DagRun=None):
        print(data_interval_start)

    example_task()
