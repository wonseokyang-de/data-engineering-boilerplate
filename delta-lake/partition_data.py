from delta import *

from spark_config import get_spark_session


spark = get_spark_session()

df.write.format('delta').partitionBy('gender').saveAsTable('default.people10m')

(DeltaTable.create(spark)
    .tableName('default.people10m')
    .addColumn('id', 'INT')
    .addColumn('firstName', 'STRING')
    .addColumn('middleName', 'STRING')
    .addColumn('lastName', 'STRING', comment='surname')
    .addColumn('gender', 'STRING')
    .addColumn('birthDate', 'TIMESTAMP')
    .addColumn('ssn', 'STRING')
    .addColumn('salary', 'INT')
    .partitionBy('gender')
    .execute()
)
