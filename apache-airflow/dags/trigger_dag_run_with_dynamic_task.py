from airflow import DAG
from airflow.models import DagRun
from airflow.decorators import task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

with DAG(
    dag_id='trigger_dag_run_with_dynamic_task',
    default_args={
        'owner': 'Wonseok Yang',
        'retries': 0
    },
    schedule_interval=None
) as dag:

    @task
    def generate_kwargs(data_interval_end=None):
        trigger_dag_run_kwargs = []
        for i in range(3):
            base_kwargs = {
                'execution_date': data_interval_end.add(microseconds=i),
            }

            dag_run_kwargs = []
            for table in ['ea01', 'eb01', 'ec01']:
                dag_run_kwargs.append({
                    'table': table
                })

            base_kwargs['conf']['dag_run_kwargs'] = dag_run_kwargs

            trigger_dag_run_kwargs.append(base_kwargs)
            
        return trigger_dag_run_kwargs
            
    (TriggerDagRunOperator
        .partial(
            task_id='trigger_dag_run',
            trigger_dag_id='trigger_dag_run_target',
            reset_dag_run=True)
        .expand_kwargs(generate_kwargs())
    )