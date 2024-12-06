from airflow.utils import DagRunType
from airflow.decorators import dag, task


@dag(
    dag_id='check_dag_run_type',
    ...
)
def main():
    @task
    def get_target_date_by_run_type(dag_run=None, data_interval_end=None):
        if dag_run.run_type == DagRunType.SCHEDULED:
            return data_interval_end.in_timezone('Asia/Seoul').subtract(months=1).strftime('%Y%m') + '01'
        elif dag_run.run_type == DagRunType.MANUAL:
            return dag_run.conf.get('target_date', '20241211')

    run_config = {
        'target_date': get_target_date_by_run_type
    }


dag = main()
