from delta import *

from spark_config import get_spark_session


spark = get_spark_session()

df = spark.read.format('delta').option('versionAsOf', 0).load('/datalake/delta-table-version0')

df.show()
