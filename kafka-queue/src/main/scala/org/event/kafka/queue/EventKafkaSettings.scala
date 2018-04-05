package org.event.kafka.queue

import org.apache.commons.cli.{BasicParser, CommandLine, CommandLineParser, Options}

final object EventKafkaSettings {

  val MAX_METADATA_VALUE = 100

  private val loadSettings = new EventKafkaSettings ()
  private val streamSettings = new EventKafkaSettings ()

  def setLoadSettings (args: Array[String]): Unit = {
    val options: Options = new Options

    options.addOption("tableName", true, "Name of the table to use in the Event Store")
    options.addOption("localBroker", true, "Using a local Kafka Broker: true|false")
    options.addOption("kafkaBroker", true, "location of the Kafka Broker: localhost:9092")
    options.addOption("topic", true, "String name for the Kafka topic")
    options.addOption("group", true, "String name for the Kafka group")
    options.addOption("metadata", true, "String Name for the metadata: Sensor, ...")
    options.addOption("metadataId", true, "Long Value for the metadata: 238")
    options.addOption("batchSize", true, "Size of the batch to upload on the cluster")

    val parser: CommandLineParser = new BasicParser
    val cmd: CommandLine = parser.parse(options, args)

    try {
      loadSettings.setTableName(cmd.getOptionValue("tableName"))
      loadSettings.setLocalBroker(cmd.getOptionValue("localBroker").toBoolean)
      loadSettings.setBroker(cmd.getOptionValue("kafkaBroker"))
      loadSettings.setTopic(cmd.getOptionValue("topic"))
      loadSettings.setGroup(cmd.getOptionValue("group"))
      loadSettings.setMetadata(cmd.getOptionValue("metadata"))
      loadSettings.setMetadataId(Integer.parseInt(cmd.getOptionValue("metadataId")))
      loadSettings.setBatchSize(Integer.parseInt(cmd.getOptionValue("batchSize")))
    }
    catch {
      case e: Exception => {
        throw new IllegalArgumentException("Issue in passing arguments - Requires -tableName <> -kafkaBroker -topic <> -group <> -metadata <> -metadataId <> -batchSize <>")
      }
    }
  }

  def setStreamSettings (args: Array[String]): Unit = {
    val options: Options = new Options

    options.addOption("localBroker", true, "Using a local Kafka Broker: true|false")
    options.addOption("kafkaBroker", true, "location of the Kafka Broker: localhost:9092")
    options.addOption("topic", true, "String name for the Kafka topic")
    options.addOption("eventStore", true, "Event Store configuration String")
    options.addOption("user", true, "UserName for the Event Store")
    options.addOption("password", true, "Password for the Event Store")
    options.addOption("streamingInterval", true, "Streaming window for each batch")
    options.addOption("database", true, "String name for the Event Store Database")
    options.addOption("batchSize", true, "Size of the batch to send to the IBM Db2 Event Store")
    options.addOption("metadata", true, "String Name for the metadata: Sensor, ...")

    val parser: CommandLineParser = new BasicParser
    val cmd: CommandLine = parser.parse(options, args)

    try {
      streamSettings.setLocalBroker(cmd.getOptionValue("localBroker").toBoolean)
      streamSettings.setBroker(cmd.getOptionValue("kafkaBroker"))
      streamSettings.setTopic(cmd.getOptionValue("topic"))
      streamSettings.setEventStore(cmd.getOptionValue("eventStore"))
      streamSettings.setUser(cmd.getOptionValue("user"))
      streamSettings.setPassword(cmd.getOptionValue("password"))
      streamSettings.setStreamingInterval(Integer.parseInt(cmd.getOptionValue("streamingInterval")))
      streamSettings.setDatabase(cmd.getOptionValue("database"))
      streamSettings.setBatchSize(Integer.parseInt(cmd.getOptionValue("batchSize")))
      streamSettings.setMetadata(cmd.getOptionValue("metadata"))
    }
    catch {
      case e: Exception => {
        throw new IllegalArgumentException("Issue in passing arguments - Requires -kafkaBroker -topic <> -eventStore <> -user <> -password <> -streamingInterval <> -database <> -batchSize <>")
      }
    }
  }

  def getLoadSettings (): EventKafkaSettings = loadSettings
  def getStreamSettings (): EventKafkaSettings = streamSettings
}

class EventKafkaSettings {
  var localBroker: Boolean = true
  var kafkaBrokers = "localhost:9092"
  var eventStoreKafkaGroup = "eventStoreGroup"
  var eventStoreKafkaTopic = "eventStoreTopic"
  var eventStore = ""
  var user = "admin"
  var password = "password"
  var database = ""
  var appName = "Db2 Event Store Kafka Connector"
  var tableName = ""
  var metadata = ""
  var metadataId = 0L
  var eventStoreKafkaListenerStreamingInteral = 5000
  var kafkaBatchSize = 1000
  var SensorMaxValue = 100

  def setLocalBroker(localBroker: Boolean): Unit = {this.localBroker = localBroker}
  def setTableName(tableName: String): Unit = {this.tableName = tableName}
  def setDatabase(databaseName: String): Unit = {this.database = databaseName}
  def setBroker(broker: String): Unit = {this.kafkaBrokers = broker}
  def setTopic(topic: String): Unit = {this.eventStoreKafkaTopic = topic}
  def setGroup(group: String): Unit = {this.eventStoreKafkaGroup = group}
  def setMetadata(metadata: String): Unit = {this.metadata = metadata}
  def setMetadataId(metadataID: Long): Unit = {this.metadataId = metadataID}
  def setBatchSize(batchSize: Int): Unit = {this.kafkaBatchSize = batchSize}
  def setEventStore(eventStore: String): Unit = {this.eventStore = eventStore}
  def setUser(user: String): Unit = {this.user = user}
  def setPassword(password: String): Unit = {this.password = password}
  def setStreamingInterval(streamingInterval: Int): Unit = {this.eventStoreKafkaListenerStreamingInteral = streamingInterval}

  def getLocalBroker(): Boolean = this.localBroker
  def getTableName(): String = this.tableName
  def getDatabase(): String = this.database
  def getBroker(): String = this.kafkaBrokers
  def getTopic(): String = this.eventStoreKafkaTopic
  def getGroup(): String = this.eventStoreKafkaGroup
  def getMetadata(): String = this.metadata
  def getMetadataId(): Long = this.metadataId
  def getBatchSize(): Long = this.kafkaBatchSize
  def getEventStore(): String = this.eventStore
  def getUser(): String = this.user
  def getPassword(): String = this.password
  def getStreamingInterval(): Long = this.eventStoreKafkaListenerStreamingInteral
}