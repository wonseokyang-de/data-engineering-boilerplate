from pyspark.sql.functions import *

from spark_config import get_spark_session

spark = get_spark_session()

source_df = spark.read.format('delta').load('s3://some_bucket/data/some_table')

target_df = spark.read.format('delta').load('s3://some_bucket/data/some_table')

final_target_to_delete_df = (target_df
    .join(
        source_df,
        on='target_to_delete',
        how='anti'
    )
)

