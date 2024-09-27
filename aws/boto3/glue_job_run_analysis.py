import boto3
import pandas as pd
from itertools import islice

glue = boto3.client('glue')
paginator = glue.get_paginator('get_job_runs')

glue_job_name = 'some_job'

pages = paginator.paginate(JobName=glue_job_name)

job_runs = []
for page in islice(pages, 10):
    job_runs.extend(page['JobRuns'])

raw_job_runs_df = pd.DataFrame(job_runs['some_key'])

# Change timezone to KST
...

# Filter date 
# 1. Timestamp -> df[col].dt.date
# 2. Timestamp(target_date).date()
...

# Change column format CamelCase to snake_case with re module
...

# Explode nested column like Arguments
...

# Save to S3 with PyArrow
...
