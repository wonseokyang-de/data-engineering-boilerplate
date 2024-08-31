from delta.tables import *
from pyspark.sql.functions import *

from spark_config import get_spark_session


spark = get_spark_session()

data = spark.range(5, 10)
data.write.format('delta').save('datalake/delta-table')

delta_table = DeltaTable.forPath(spark, 'datalake/delta-table')

delta_table.update(
    condition=expr('id % 2 == 0'),
    set={'id': expr('id + 100')
})

delta_table.delete(condition=expr('id % 2 == 0'))

new_data = spark.range(0, 20)

(delta_table.alias('old_data')
    .merge(
        new_data.alias('new_data'),
        'old_data.id = new_data.id')
    .whenMatchedUpdate(set={'id': col('new_data.id')})
    .whenNotMatchedInsert(values={'id': col('new_data.id')})
    .execute()
)

delta_table.toDF().show()
