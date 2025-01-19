from airflow import DAG
from airflow.decorators import task


with DAG(
    dag_id='test',
    default_args={
        'owner': 'Wonseok Yang',
        'retries': 0
    },
    schedule_interval=None
) as dag:

    @task
    def example_task(data_interval_start=None):
        print(data_interval_start)

    example_task()
