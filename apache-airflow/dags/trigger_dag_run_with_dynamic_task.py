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
        target_tables = ['ea01', 'eb01', 'ec01']

        trigger_dag_run_kwargs = []
        for table in target_tables:
            _trigger_dag_run_kwargs = []
            for i in range(5):
                _trigger_dag_run_kwargs.append({
                    'execution_date': data_interval_end.add(microseconds=i),
                    'conf': {
                        'index': i,
                        'table': table,
                        'data': f'data_{i}'
                    }
                })
            trigger_dag_run_kwargs.append(_trigger_dag_run_kwargs)
            
        return trigger_dag_run_kwargs
            
    (TriggerDagRunOperator
        .partial(
            task_id='trigger_dag_run',
            trigger_dag_id='trigger_dag_run_target',
            reset_dag_run=True)
        .expand_kwargs(generate_kwargs())
    )