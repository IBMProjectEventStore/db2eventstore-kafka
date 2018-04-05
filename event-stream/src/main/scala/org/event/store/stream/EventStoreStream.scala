package org.event.store.stream

import org.apache.kafka.common.serialization.{ByteArrayDeserializer, ByteArraySerializer}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.event.kafka.queue._

object  EventStoreStream {
  def main(args: Array[String]): Unit = {

    EventKafkaSettings.setStreamSettings(args)
    val streamSettings = EventKafkaSettings.getStreamSettings()
    val SparkCheckpointDir = "./checkpoints/"

    // Create embedded Kafka and topic
    if (streamSettings.getLocalBroker()) {
      val kafka = KafkaLocalServer(true)
      kafka.start()
      kafka.createTopic(streamSettings.getTopic())
      println(s"Kafka Cluster created for topic ${streamSettings.getTopic()}")
    }

    val spark = SparkSession
      .builder()
      .appName("Event Store Stream Application")
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    val ssc = new StreamingContext(spark.sparkContext, Seconds(streamSettings.getStreamingInterval() / 1000))
    ssc.checkpoint(SparkCheckpointDir)

    val kafkaParams = MessageListener.consumerProperties(
      streamSettings.getBroker(),
      streamSettings.getGroup(), classOf[ByteArrayDeserializer].getName, classOf[ByteArrayDeserializer].getName
    )
    val topics = List(streamSettings.getTopic())

    /**
      * Initialize the Event Store Streaming Sink
      */
    val eventStoreSink = spark.sparkContext.broadcast(EventStoreConnector(streamSettings))

    val kafkaSinkProps = MessageProducer.producerProperties(streamSettings.getBroker(),
      classOf[ByteArraySerializer].getName, classOf[ByteArraySerializer].getName)
    val kafkaSink = spark.sparkContext.broadcast(KafkaSink(kafkaSinkProps))

    val kafkaDataStream = KafkaUtils.createDirectStream[Array[Byte], Array[Byte]](
      ssc, PreferConsistent, Subscribe[Array[Byte], Array[Byte]](topics, kafkaParams)
    )
    val rawStream = kafkaDataStream.map(r => EventStoreEvent(r.value()))

    /** Stream the kafka data the to Event Store */
    rawStream.foreachRDD {spark.createDataFrame(_).foreachPartition(eventStoreSink.value.streamData(_)) }

    /**
      * Start streaming context
      */
    ssc.start()
    ssc.awaitTermination()
  }
}
