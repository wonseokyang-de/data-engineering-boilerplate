from pyspark.sql import SparkSession
from pyspark.sql.functions import window, col


spark = (SparkSession.builder
	.appName("KafkaToS3Batch")
	.getOrCreate()
)

df = (spark.readStream
	.format("kafka")
	.option("kafka.bootstrap.servers", "your_kafka_server:port")
	.option("subscribe", "your_topic")
	.option("startingOffsets", "earliest")
	.load()
)

df = df.selectExpr("CAST(value AS STRING)")

window_count_df = (df
	.groupBy(window(col("timestamp"), "30 minutes"))
    .count()
)

query = (window_count_df
	.writeStream
	.outputMode("complete")
	.format("console")
	.option("truncate", "false")
	.start()
)

query.awaitTermination()
