# Create table in the metastore using DataFrame's schema and write dasta to it
df.write.format('delta').saveAsTable('default.people10m')

# Create or replace partitioned table with path using DataFrame's schema and write/overwrite data to it
df.write.format('delta').mode('overwrite').save('./datalake/delta/people10m')
