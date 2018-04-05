package org.event.store.stream

trait EventStoreStreamingRecord extends Product with Serializable

case class EventStoreEvent (row: Array[Byte]) extends EventStoreStreamingRecord