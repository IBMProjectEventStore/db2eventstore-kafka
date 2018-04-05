package org.event.kafka.queue

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

object KafkaSink{
  def apply(config: Properties): KafkaSink = {
    val f = () => {
      val producer = new KafkaProducer[Array[Byte], Array[Byte]](config)
      sys.addShutdownHook {
        producer.close()
      }
      producer
    }
    new KafkaSink(f)
  }
}

class KafkaSink(createProducer: () => KafkaProducer[Array[Byte], Array[Byte]]) extends Serializable{

  lazy val producer = createProducer()

  def send(topic: String, key: Array[Byte], value: Array[Byte]): Unit = {
    producer.send(new ProducerRecord(topic, key, value))
  }

  def send(topic: String, value: Array[Byte]): Unit = {
    producer.send(new ProducerRecord(topic, null, value))
  }
}
