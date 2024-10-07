from airflow import DAG
from airflow.models import DagRun
from airflow.operators.empty import EmptyOperator
from airflow.utils import task
from airflow.utils import task_group


with DAG(
    dag_id='branching_with_conf',
    owner='Wonseok Yang'
) as dag:

    @task
    def check_configuration_from_airflow_web(dag_run: DagRun=None):
        required_keys = ['layer', 'base_date', 'target_tables']

        if key not in required_keys:
            raise KeyError("Check configuration key")

    @task.branch
    def choose_task_from_conf_value(dag_run: DagRun=None):
        return dag_run.conf.get('layer')

    task_1 = EmptyOperator(task_id='task_1')
    task_2 = EmptyOperator(task_id='task_2')

    (
        check_configuration_from_airflow_web()
        >> choose_task_from_conf_value()
        >> [task_1, task_2]
    )
