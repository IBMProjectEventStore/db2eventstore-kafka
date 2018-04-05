package org.event.kafka.queue

import java.util.Properties

import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.clients.producer.ProducerConfig

import scala.collection.JavaConversions._

object MessageListener {
  private val AUTOCOMMITINTERVAL = "1000" // Frequency off offset commits
  private val SESSIONTIMEOUT = "30000" // The timeout used to detect failures - should be greater then processing time
  private val MAXPOLLRECORDS = "10" // Max number of records consumed in a single poll

  def consumerProperties(brokers: String, group: String, keyDeserealizer: String, valueDeserealizer: String): Map[String, String] = {
    Map[String, String](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers,
      ConsumerConfig.GROUP_ID_CONFIG -> group,
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "true",
      ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG -> AUTOCOMMITINTERVAL,
      ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG -> SESSIONTIMEOUT,
      ConsumerConfig.MAX_POLL_RECORDS_CONFIG -> MAXPOLLRECORDS,
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "latest",
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> keyDeserealizer,
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> valueDeserealizer
    )
  }

  def apply[K, V](brokers: String, topic: String, group: String, keyDeserealizer: String, valueDeserealizer: String,
                  processor: RecordProcessorTrait[K, V]): MessageListener[K, V] =
    new MessageListener[K, V](brokers, topic, group, keyDeserealizer, valueDeserealizer, processor)
}

class MessageListener[K, V](brokers: String, topic: String, group: String, keyDeserealizer: String, valueDeserealizer: String,
                            processor: RecordProcessorTrait[K, V]) extends Runnable {

  import MessageListener._
  val consumer = new KafkaConsumer[K, V](consumerProperties(brokers, group, keyDeserealizer, valueDeserealizer))
  consumer.subscribe(Seq(topic))
  var completed = false

  def complete(): Unit = {
    completed = true
  }

  override def run(): Unit = {
    while (!completed) {
      val records = consumer.poll(100)
      import scala.collection.JavaConversions._
      for (record <- records) {
        processor.processRecord(record)
      }
    }
    consumer.close()
    System.out.println("Listener completes")
  }

  def start(): Unit = {
    val t = new Thread(this)
    t.start()
  }
}

object MessageProducer{

  private val ACKCONFIGURATION = "all" // Blocking on the full commit of the record
  private val RETRYCOUNT = "1" // Number of retries on put
  private val BATCHSIZE = "1024" // Buffers for unsent records for each partition - controlls batching
  private val LINGERTIME = "1" // Timeout for more records to arive - controlls batching
  private val BUFFERMEMORY = "1024000" // Controls the total amount of memory available to the producer for buffering. If records are sent faster than they can be transmitted to the server then this buffer space will be exhausted. When the buffer space is exhausted additional send calls will block. The threshold for time to block is determined by max.block.ms after which it throws a TimeoutException.

  def producerProperties(brokers: String, keySerealizer: String, valueSerealizer: String): Properties = {
    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    props.put(ProducerConfig.ACKS_CONFIG, ACKCONFIGURATION)
    props.put(ProducerConfig.RETRIES_CONFIG, RETRYCOUNT)
    props.put(ProducerConfig.BATCH_SIZE_CONFIG, BATCHSIZE)
    props.put(ProducerConfig.LINGER_MS_CONFIG, LINGERTIME)
    props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, BUFFERMEMORY)
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerealizer)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerealizer)
    props
  }
}