package org.event.data.load

import org.apache.kafka.common.serialization.ByteArraySerializer
import org.event.kafka.queue.{EventKafkaSettings, MessageSender}

import scala.collection.mutable.ListBuffer

object DataLoad {

  private val timeInterval: Long = 1000 * 1 // 1 s

  def main(args: Array[String]) {

    EventKafkaSettings.setLoadSettings(args)
    val loadSettings = EventKafkaSettings.getLoadSettings()
    DataLoad(loadSettings).execute()

  }

  def pause(time : Long): Unit = {
    try {
      Thread.sleep(time)
    } catch {
      case _: Throwable =>
    }
  }

  def apply(settings: EventKafkaSettings): DataLoad = new DataLoad(settings)
}

class DataLoad(settings: EventKafkaSettings) {

  private var sender = MessageSender[Array[Byte], Array[Byte]](settings.getBroker(), classOf[ByteArraySerializer].getName, classOf[ByteArraySerializer].getName)
  private val rand = scala.util.Random
  private val jsonData = new JsonData()
  rand.setSeed(System.currentTimeMillis())

  def execute(): Unit = {

    var numRec: Long = 1
    while (true) {
      val batch = new ListBuffer[Array[Byte]]()
      var count = 0
      while (count < settings.getBatchSize()) {
        batch += jsonData.getMessage(settings.getTableName(), numRec, settings.getMetadata(), settings.getMetadataId(), rand.nextInt(EventKafkaSettings.MAX_METADATA_VALUE))
        numRec += 1
        count += 1
      }
      try {
        if (sender == null) {
          sender = MessageSender[Array[Byte], Array[Byte]](settings.getBroker(), classOf[ByteArraySerializer].getName, classOf[ByteArraySerializer].getName)
        }
        sender.batchWriteValue(settings.getTopic(), batch)
        batch.clear()
      } catch {
        case e: Throwable =>
          println(s"Kafka failed: ${e.printStackTrace()}")
          if (sender != null) {
            sender.close()
          }
          sender = null
      }
      println(s"Submitted ${settings.getBatchSize()} records")
      DataLoad.pause(DataLoad.timeInterval)
    }
  }
}