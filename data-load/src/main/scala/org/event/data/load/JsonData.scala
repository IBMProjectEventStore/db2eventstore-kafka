package org.event.data.load

import org.event.kafka.queue.EventKafkaSettings._

class JsonData {

  def getMessage (tableName: String, numRec: Long, metadata: String, metadataId: Long, value: Long) : Array[Byte] = {
    s"""{"table":"${tableName}", "payload":{"id": ${numRec}, "${metadata}": ${metadataId}, "ts":${System.nanoTime()}, "value":${value}}}""".getBytes
  }
}
