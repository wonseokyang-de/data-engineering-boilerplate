from pyspark.sql import SparkSession

spark = (SparkSession.builder
    .appName("default")
    .config('spark.jars.packages', 
        'io.delta:delta-core_2.12:2.1.0,'
        'org.apache.hadoop:hadoop-aws:3.3.2,'
        'com.amazonaws:aws-java-sdk-bundle:1.11.1026')
    .config('spark.hadoop.fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem')
    .config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension')
    .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog')
    .config('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.auth.InstanceProfileCredentialsProvider')
    .config('spark.hadoop.fs.s3a.endpoint', 's3.amazonaws.com')
    .getOrCreate()
)

data = [('Alice', 34), ('Bob', 45), ('Cathy', 29)]

df = spark.createDataFrame(data, ['name', 'age'])

(df.write
    .format('delta')
    .save('s3a://deadline-datalake/test-table')
)
