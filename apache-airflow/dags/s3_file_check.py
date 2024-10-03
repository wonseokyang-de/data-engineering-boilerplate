import boto3
from airflow import DAG
from airflow.models import DagRun
from airflow.decorators import task
from airflow.providers.amazon.aws.operators.s3 import S3ListOperator
from airflow.executors.debug_executor import DebugExecutor


executor = DebugExecutor()

with DAG(
    dag_id='s3_file_check',
    default_args={
        'owner': 'Wonseok Yang',
        'retries': 0
    },
    schedule_interval=None
) as dag:

    @task
    def example_task(data_interval_start=None):
        s3_client = boto3.client('s3')

        print(data_interval_start)

    example_task()


if __name__ == '__main__':
    dag.clear()
    dag.run(executor=executor)  # DebugExecutor·Î DAG ½ÇÇà
