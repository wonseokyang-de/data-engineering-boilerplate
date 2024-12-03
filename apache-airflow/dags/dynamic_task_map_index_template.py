from airflow import DAG
from airflow.decorators import task
from airflow.providers.amazon.aws.operators.glue


with DAG(
    dag_id='dynamic_task_map_index_template',
    default_args={
        'owner': 'Wonseok Yang',
        'retries': 0
    },
    schedule_interval=None
) as dag:
    @task
    def create_args():
        return [{'arg': 'test_0'}, {'arg': 'test_1'}]

    @task
