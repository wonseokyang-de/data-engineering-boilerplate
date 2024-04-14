from pyspark.sql import SparkSession
from pyspark.sql.functions import window, col

# Spark 세션 생성
spark = (SparkSession.builder
	.appName("KafkaToS3Batch")
	.getOrCreate()
)

# Kafka 소스로부터 스트리밍 데이터프레임 읽기
df = (spark.readStream
	.format("kafka")
	.option("kafka.bootstrap.servers", "your_kafka_server:port")
	.option("subscribe", "your_topic")
	.option("startingOffsets", "earliest")
	.load()
)

# 데이터프레임을 문자열로 변환 (예제에서는 메시지의 값만 사용)
df = df.selectExpr("CAST(value AS STRING)")

# 30분 간격으로 윈도우 생성
windowedCounts = df \
	.groupBy(
	window(col("timestamp"), "30 minutes")
).count()

# 콘솔에 출력 (실제 운영에서는 이 부분을 S3에 쓰는 코드로 대체)
query = windowedCounts \
	.writeStream \
	.outputMode("complete") \
	.format("console") \
	.option("truncate", "false") \
	.start()

query.awaitTermination()
