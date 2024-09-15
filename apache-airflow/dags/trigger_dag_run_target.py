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
    def parse_dag_run_kwargs(dag_run=None):
        return dag_run.conf['dag_run_kwargs']
    
    @task
    def print_kwargs(table):
        print(table)

    print_kwargs.partial().expand_kwargs(parse_dag_run_kwargs())
    