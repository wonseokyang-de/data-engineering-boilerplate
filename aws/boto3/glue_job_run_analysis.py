import boto3

glue = boto3.client('glue')
paginator = glue.get_paginator('get_job_runs')

glue_job_name = 'some_job'

job_runs = []
for page in paginator.paginate(JobName=glue_job_name):
    job_runs.extend(page['JobRuns'])


job_runs[0]