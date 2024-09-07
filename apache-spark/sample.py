from pyspark.sql.functions import *

from spark_config import get_spark_session


# spark = get_spark_session()
#
# spark

import boto3

s3 = boto3.client('s3')

print(s3.list_buckets())
