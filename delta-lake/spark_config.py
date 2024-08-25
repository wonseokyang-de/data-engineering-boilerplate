from pyspark.sql import SparkSession

def get_spark_session(app_name='default'):
    return (SparkSession.builder
        .appName(app_name)
        .config('spark.jars.packages', 
            'io.delta:delta-core_2.12:2.1.0,'
            'org.apache.hadoop:hadoop-aws:3.3.1,'
            'com.amazonaws:aws-java-sdk-bundle:1.12.375')
        .config('spark.hadoop.fs.s3.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem')
        .config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension')
        .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog')
        .config("spark.hadoop.fs.s3.aws.credentials.provider", "com.amazonaws.auth.InstanceProfileCredentialsProvider")
        .config('spark.hadoop.fs.s3.endpoint', 's3.amazon.com')
        .getOrCreate()
    )
