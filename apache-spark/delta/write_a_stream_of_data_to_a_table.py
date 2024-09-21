streaming_df = spark.readStream.format('rate').load()

stream = (streaming_df
    .selectExpr('value as id')
    .writeStream
    .format('delta')
    .option('checkpointLocation', './datalake/checkpoint')
    .start('./datalake/delta-stream-table')
)

stream_2 = (spark
    .readStream
    .format('delta')
    .load('./datalake/delta-stream-table')
    .writeStream
    .format('console')
    .start()
)
