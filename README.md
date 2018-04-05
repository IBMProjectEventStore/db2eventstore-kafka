# IBM Db2 Event Store Streaming Connector for Kafka

## Pre-requisites
```
Install sbt at the version 0.13.16
git clone git@github.com:IBMProjectEventStore/db2eventstore-kafka.git
cd db2eventstore-kafka
```

check your sbt version with:
```
sbt sbtVersion
```
Output:
...
[info] kafkaQueue/*:sbtVersion
[info] 	0.13.16
[info] dataLoad/*:sbtVersion
[info] 	0.13.16
[info] eventStream/*:sbtVersion
[info] 	0.13.16
[info] db2eventstoreKafka/*:sbtVersion
[info] 	0.13.16

## Compile
```
sbt clean
sbt compile
```

## Package
```
sbt package assembly
```

## Start Sequence when using the built-in internal kafka broker
1. Run the Db2 Event Store Streaming Connector
2. Run the Data Generator
This order isn't necessary if using an external kafka broker

## Run the Db2 Event Store Streaming Connector

This connector could be run standalone, without the generator below. However, the row format currently needs to match the JSON, mentioned below.

### Run command
```
sbt "eventStream/run -localBroker true -kafkaBroker localhost:9092 -topic estopic -eventStore 9.30....:1101,9.30....:1101,9.30....:1101 -database TESTDB -user admin  -password password -metadata sensor -streamingInterval 5000 -batchSize 1000"
```

*Options*
- localBroker [Boolean]- true to use of an internal, single node, Kafka broker. False to use an externally configured Broker
- kafkaBroker [String]- Location of the Kafka broker, pass in "localhost:9092" for a local setup or the ip:port location for an external setup
- topic [String]- The Kafka topic to be used for the stream
- eventStore [String]- The IP configuration for the IBM Db2 Event Store
- database [String]- The Db2 Event Store Database to create or use
- user [String]- The Db2 Event Store user name to use
- password [String]- The Db2 Event Store password to use
- metadata [String]- The type of metadata for this IoT device, for instance "sensor" or "appliance" or "weatherstation"  
- streamingInterval [Long]- The Long value defining the length of the Apache Spark streaming window
- batchSize [Int] - The size of the batch to send to the IBM Db2 Event Store

## Run the Data Generator

The data generator is there to help generate some data. The generated sample data includes a type, a timestamp, a payload value.

### Kafka Broker
This example comes pre-packaged with an internal broker. However, you can also connect it to an external Kafka cluster. You will need to set the arguments localBroker accordingly
- true for having the code create an internal Broker
- false for connecting the stream to an external Broker

### Run command
```
sbt "dataLoad/run -localBroker true -kafkaBroker localhost:9092 -tableName sensor -topic estopic -group group -metadata sensor -metadataId 238 -batchSize 1000"
```

*Options*
- localBroker [Boolean]- true to use of an internal, single node, Kafka broker. False to use an externally configured Broker
- kafkaBroker [String]- Location of the Kafka broker, pass in "localhost:9092" for a local setup or the ip:port location for an external setup
- tableName [String] - The table name that will be created within the IBM Db2 Event Store
- topic [String]- The Kafka topic to be used for the stream
- group [String]- The Kafka group to be used for the stream
- metadata [String]- The type of metadata for this simulated IoT device, for instance "sensor" or "appliance" or "car", ...
- metadataId [Long]- The Long value that can identify this IoT device, for instance "238" or "002" or ...
- batchSize [Int]- The size of the batch that will be sent to the Kafka queue. It can be as small as 1.

### Underlying Json format
The current format generated for this sample generator is a JSON string, like this:
```
s"""{"table":"${tableName}", "payload":{"id": ${numRec}, "${metadata}": ${metadataId}, "timestamp":${System.currentTimeMillis()}, "value":${value}}}"""
```
- numRec is a sequence that will restart at -0- each time the generator is restarted

## IBM Db2 Event Store Table definition

Currently, the table that will be created in the event store will have the following format. It is designed to support a payload from an IoT device of type "metadata" that sends a value at a frequent interval. The timestamp will be automatically generated by the loader.

```
val tableDefinition = TableSchema(tableName, StructType(Array(
  StructField("id", LongType, nullable = false),
  StructField(s"${metadata}", LongType, nullable = false),
  StructField("timestamp", LongType, nullable = false),
  StructField("value", LongType, nullable = false)
)),
  shardingColumns = Seq("timestamp", s"${metadata}"),
  pkColumns = Seq("timestamp", s"${metadata}"))
val indexDefinition = IndexSpecification("IndexDefinition", tableDefinition, equalColumns = Seq(s"${metadata}"), sortColumns = Seq(SortSpecification("timestamp", ColumnOrder.AscendingNullsLast)), includeColumns = Seq("value"))
```

## IBM Db2 Event Store supported queries

Such a format will allow for running real-time queries such as this one below, where an index is defined on the timestamp and the metadata columns
```
SELECT value, timestamp from ${tableName} where ${metadata}=${metadataId} and timestamp>=${timestampValueinMS} and timestamp<=${timestampValueinMS}
```
