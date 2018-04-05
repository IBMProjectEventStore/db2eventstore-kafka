package org.event.store.stream

import com.ibm.event.catalog._
import com.ibm.event.common.ConfigurationReader
import com.ibm.event.oltp.{EventContext, InsertResult}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.event.kafka.queue.EventKafkaSettings

import scala.concurrent.{Await, Future}
import scala.concurrent.duration.Duration
import scala.collection.mutable.ListBuffer
import com.fasterxml.jackson.core.{JsonFactory, JsonToken}

object EventStoreConnector {

  def apply(settings: EventKafkaSettings): EventStoreConnector = {
    val f = () => {
      ConfigurationReader.setEventUser(settings.getUser())
      ConfigurationReader.setEventPassword(settings.getPassword())
      ConfigurationReader.setConnectionEndpoints(settings.getEventStore())
      ConfigurationReader.setUseFrontendConnectionEndpoints(true)
      val eventContext: Option[EventContext] = try {
        val context = EventContext.createDatabase(settings.getDatabase())
        println(s"""Creating Database ${settings.getDatabase()}""")
        Some(context)
      } catch {
        case e: Throwable => {
          try {
            val context = Some(EventContext.getEventContext(settings.getDatabase()))
            EventContext.openDatabase(settings.getDatabase())
            println(s"""Opening Database ${settings.getDatabase()}""")
            context
          } catch {
            case e: Throwable => None
          }
        }
      }
      sys.addShutdownHook {
        EventContext.cleanUp()
      }
      eventContext
    }
    new EventStoreConnector(f, settings)
  }
}

class EventStoreConnector(createContext: () => Option[EventContext], settings: EventKafkaSettings) extends Serializable {

  var ctx: Option[EventContext] = createContext()
  var table: Option[ResolvedTableSchema] = None
  var tableName: Option[String] = None
  val list = new ListBuffer [Row] ()
  val factory = new JsonFactory()
  val batchSize = settings.getBatchSize()

  def getOrCreateTable(tableName: String): Unit = {
    if (!this.table.isDefined) {
      val tables = ctx.get.getNamesOfTables
      for {
        name <- tables
        if (name == this.tableName.get)
      } this.table = Some (ctx.get.getTable(tableName))

      if (!this.table.isDefined) {
        val tableDefinition = TableSchema(tableName, StructType(Array(
          StructField("id", LongType, nullable = false),
          StructField(s"${settings.getMetadata()}", LongType, nullable = false),
          StructField("timestamp", LongType, nullable = false),
          StructField("value", LongType, nullable = false)
        )),
          shardingColumns = Seq("timestamp", s"${settings.getMetadata()}"),
          pkColumns = Seq("timestamp", s"${settings.getMetadata()}"))
        val indexDefinition = IndexSpecification("IndexDefinition", tableDefinition, equalColumns = Seq(s"${settings.getMetadata()}"), sortColumns = Seq(SortSpecification("timestamp", ColumnOrder.AscendingNullsLast)), includeColumns = Seq("value"))
        val status = ctx.get.createTableWithIndex(tableDefinition, indexDefinition)
        if (!status.isDefined) {
          this.table = Some(ctx.get.getTable(tableName))
        }
        else {
          println(s"""Error Creating table ${tableName} with error ${status.get.errStr}""")
        }
      }
    }
  }

  private def generateRow(event: Event): Row = {
    Row.fromSeq(Seq(event.id, event.metadataId, event.timestamp, event.value))
  }

  def parseJSON (message: String): Event = {
    val event = new Event()
    val parser  = factory.createParser(message)

    while (!parser.isClosed) {
      val jsonToken = parser.nextToken()
      if (JsonToken.FIELD_NAME.equals(jsonToken)) {
        val fieldName = parser.getCurrentName
        parser.nextToken()
        if (!this.tableName.isDefined && "table".equals(fieldName)) {this.tableName = Some(parser.getValueAsString)}
        else if ("id".equals(fieldName)) {event.id = parser.getValueAsLong}
        else if (s"${settings.getMetadata()}".equals(fieldName)) {event.metadataId = parser.getValueAsLong}
        else if ("timestamp".equals(fieldName)) {event.timestamp = parser.getValueAsLong}
        else if ("value".equals(fieldName)) {event.value = parser.getValueAsLong}
      }
    }
    event
  }

  def streamData(iterator: Iterator[Row]): Unit = {
    for (row <- iterator) {
      val r = row.getAs[Array[Byte]](0)
      val jsonData = r.map(_.toChar).mkString
      if (jsonData != null && jsonData != "") {
        this.list += generateRow(this.parseJSON(jsonData))
        if (this.list.size == batchSize) {
          println(s"""About to flush the list and send the batch to the Db2 Event Store""")
          this.getOrCreateTable(this.tableName.get)
          writeBatch(this.table.get.tableName, this.list.iterator)
          list.clear()
        }
      }
    }
  }

  private def writeBatch(tableName: String, data: Iterator[Row]): Unit = {
    val dataSeq = data.toIndexedSeq
    val start = System.currentTimeMillis()
    if (dataSeq.size > 0) {
      try {
        val future: Future[InsertResult] = ctx.get.batchInsertAsync(tableName, dataSeq, true)
        val result: InsertResult = Await.result(future, Duration.Inf)
        if (result.failed) {
          println(s"batch insert was incomplete: $result")
        }
      } catch {
        case t: Throwable =>
          printf(s"Error writing to the IBM Db2 Event Store $t")
          ctx = None
      }
      println(s"Done inserting batch in table $tableName in ${System.currentTimeMillis() - start} ms")
    }
  }
}
