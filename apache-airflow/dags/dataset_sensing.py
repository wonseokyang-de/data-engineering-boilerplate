from airflow import DAG
from airflow.decorators import task
from airflow.datasets import Dataset


example_dataset = Dataset('s3://some-bucket/example.csv')

with DAG(
    dag_id='taskflowapi',
    default_args={
        'owner': 'Wonseok Yang',
        'retries': 0
    },
    schedule=[example_dataset],
) as dag:

    @task
    def example_task(data_interval_start=None):
        print(data_interval_start)

    example_task()
