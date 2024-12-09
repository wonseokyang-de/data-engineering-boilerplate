from airflow.decorators import dag, task


@dag(
    dag_id='trigger_dag_run_target',
    default_args={
        'owner': 'Wonseok Yang',
        'retries': 0
    },
    schedule_interval=None
)
def main():
    @task
    def get_dagrun_conf(dag_run=None):
        return dag_run.conf['dag_run_kwargs']
    
    @task
    def print_kwargs(table):
        print(table)

    (print_kwargs
        .partial()
        .expand_kwargs(get_dagrun_conf())
    )
    
dag = main()
