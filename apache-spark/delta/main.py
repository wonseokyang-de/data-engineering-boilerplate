from delta import *
from pyspark.sql import SparkSession

spark = (SparkSession.builder
    .appName('default')
    .config('spark.jars.packages', 'io.delta:delta-core_2.12:2.1.0')
    .config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension')
    .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog')
    .getOrCreate()
)

data = [('Alice', 34), ('Bob', 45), ('Cathy', 29)]

df = spark.createDataFrame(data, ['name', 'age'])

(df.write.format('delta')
    .save('./datalake/people')
)
