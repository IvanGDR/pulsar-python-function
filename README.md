# pulsar-python-function

For this python function I am using a DataStax CDC source connector and DataStax sink connector.

The pulsar python function will filter messages from the data-<keyspace name>.<table name> topic to another topic named filtered_topic.
  
  
### SOURCE TOPIC: data-cdckeyspace.cdctable topic 
  
using DataStax CDC agent, the table is defined like this:

 ```
CREATE TABLE cdckeyspace.cdctable (
    ip text PRIMARY KEY,
    status text
) WITH cdc = true;
```
  
This topic has avro schema
```
$./bin/pulsar-admin schemas get data-cdckeyspace.cdctable
```
  
```
{
  "version": 0,
  "schemaInfo": {
    "name": "data-cdckeyspace.cdctable",
    "schema": {
      "key": {
        "name": "cdctable",
        "schema": {
          "type": "record",
          "name": "cdctable",
          "namespace": "cdckeyspace",
          "doc": "Table cdckeyspace.cdctable",
          "fields": [
            {
              "name": "ip",
              "type": "string"
            }
          ]
        },
        "type": "AVRO",
        "properties": {}
      },
      "value": {
        "name": "cdctable",
        "schema": {
          "type": "record",
          "name": "cdctable",
          "namespace": "cdckeyspace",
          "doc": "Table cdckeyspace.cdctable",
          "fields": [
            {
              "name": "status",
              "type": [
                "null",
                "string"
              ]
            }
          ]
        },
        "type": "AVRO",
        "properties": {}
      }
    },
    "type": "KEY_VALUE",
    "properties": {
      "key.schema.name": "cdctable",
      "key.schema.properties": "{}",
      "key.schema.type": "AVRO",
      "kv.encoding.type": "SEPARATED",
      "value.schema.name": "cdctable",
      "value.schema.properties": "{}",
      "value.schema.type": "AVRO"
    }
  }
}
```
  
### SINK TOPIC: filetered_topic topic

The table definition is as follows
  
```
CREATE TABLE cdckeyspace.filtered_table (
    ip text PRIMARY KEY,
    filtered_status text
) WITH cdc = true;
```
  
The dse_sink.yml, following and AVRO mapping
  
```
configs:
  verbose: false
  batchSize: 3000
  batchFlushTimeoutMs: 1000
  topics: filtered_topic
  contactPoints: 10.101.35.73
  loadBalancing.localDc: Cassandra
  port: 9042
  cloud.secureConnectBundle:
  ignoreErrors: None
  maxConcurrentRequests: 500
  maxNumberOfRecordsInBatch: 32
  queryExecutionTimeout: 30
  connectionPoolLocalSize: 4
  jmx: true
  compression: None
  topic:
    filtered_topic:
      cdckeyspace:
        filtered_table:
          mapping: ‘ip=key.ip,filtered_status=value.filtered_status’
          consistencyLevel: LOCAL_ONE
          ttl: -1
          ttlTimeUnit : SECONDS
          timestampTimeUnit : MICROSECONDS
          nullToUnset: true
          deletesEnabled: true
      codec:
        locale: en_US
        timeZone: UTC
        timestamp: CQL_TIMESTAMP
        date: ISO_LOCAL_DATE
        time: ISO_LOCAL_TIME
        unit: MILLISECONDS
```

To send messages to the "sink" table, using "pulsar-client produce" the following is executed:
```
./bin/pulsar-client produce -k '{"ip":"18345"}', -m '{"filtered_status":"off"}' persistent://public/default/filtered_topic
```

