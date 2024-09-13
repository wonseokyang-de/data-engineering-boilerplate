from airflow import DAG
from airflow.decorators import task

with DAG(
    dag_id='trigger_dag_run_target',
    default_args={
        'owner': 'Wonseok Yang',
        'retries': 0
    },
    schedule_interval=None
) as dag:

    @task
    def get_dag_run_kwargs(dag_run=None):
        return dag_run.conf.get('trigger_list')
    
    @task
    def print_kwargs(trigger_list):
        print(trigger_list)

    print_kwargs.partial(
        task_id='print_kwargs',
    ).expand(get_dag_run_kwargs())
    