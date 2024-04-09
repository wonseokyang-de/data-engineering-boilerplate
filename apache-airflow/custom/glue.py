# Deprecated: This script is working on airflow version 2.2.2
# SO YOU MUST CHANGE TO AwsGlue -> Glue in Operator/Sensor
# And create_glue_job_task_group is helping to wait job end. That is not helpful. -> GlueJobOperator can do to wait job completion.

from airflow.utils.task_group import TaskGroup
from airflow.providers.amazon.aws.sensors.glue import AwsGlueJobSensor
from airflow.providers.amazon.aws.operators.glue import AwsGlueJobOperator


def create_glue_job_task_group(*, group_id, job_name, script_args, **kwargs):

    with TaskGroup(group_id=group_id) as task_group:
        run_glue_job = AwsGlueJobOperator(
            task_id='run_glue_job',
            job_name=job_name,
            script_args=script_args,
            **kwargs
        )
        get_glue_job_status = AwsGlueJobSensor(
            task_id='get_glue_job_status',
            job_name=job_name,
            run_id=run_glue_job.output,
        )

        run_glue_job >> get_glue_job_status

    return task_group