package org.event.store.stream

class Event () {
  var id: Long = 0L
  var metadataId: Long = 0L
  var timestamp: Long = 0L
  var value: Long = 0L

  def setId (id: Long): Unit = {this.id = id}
  def setMetadataId (metadataId: Long): Unit =  {this.metadataId = metadataId}
  def setTimestamp (timestamp: Long): Unit =  {this.timestamp = timestamp}
  def setValue (value: Long): Unit =  {this.value = value}
}
