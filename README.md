# kafka-connect-phoenix

## Phoenix Sink Connector for Kafka-connect
* Only supported message format is JSON with schema, make sure you configure kafka producer with appropriate properties
* This derives table columns based on the schema of the message received
* Batch 100 records at a time for a given poll cycle


Configurations

Below are the properties that need to be passed in the configuration file:

name | data type | required | description
-----|-----------|----------|------------
pqs.url | string | yes | Phoenix Query Server URL [http:\\host:8765]
topics | string | yes | list of kafka topics.
hbase.table | string | yes | Pphoenix table name
phoenix.fields | string | yes | phoenix table fields
