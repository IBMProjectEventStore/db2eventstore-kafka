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

      // Only set SSL parameters if they are non-empty
      if( settings.getTrustStoreLocation().nonEmpty ){
        ConfigurationReader.setSslTrustStoreLocation(settings.getTrustStoreLocation())
        ConfigurationReader.setSslTrustStorePassword(settings.getTrustStorePassword())
        ConfigurationReader.setSslKeyStoreLocation(settings.getKeyStoreLocation())
        ConfigurationReader.setSslKeyStorePassword(settings.getKeyStorePassword())
        ConfigurationReader.setClientPluginName(settings.getClientPluginName())
        ConfigurationReader.setClientPlugin(true)
        ConfigurationReader.setSSLEnabled(true)
      }

      val eventContext: Option[EventContext] = 
        try {
          val context = Some(EventContext.getEventContext(settings.getDatabase()))
          EventContext.openDatabase(settings.getDatabase())
          println(s"""Opening Database ${settings.getDatabase()}""")
          context
        } catch {
          case e: Throwable => None
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
  var schemaName: Option[String] = None
  val list = new ListBuffer [Row] ()
  val factory = new JsonFactory()
  val batchSize = settings.getBatchSize()

  def getOrCreateTable(tableName: String): Unit = {
    if (!this.table.isDefined) {
      val tables = ctx.get.getNamesOfTables
      for {
        name <- tables
        if (name == this.schemaName.get.toUpperCase + "." + this.tableName.get.toUpperCase)
      } this.table = Some (ctx.get.getTable(tableName))

      if (!this.table.isDefined) {
        val tableDefinition = TableSchema(tableName, StructType(Array(
          StructField("id", LongType, nullable = false),
          StructField(s"${settings.getMetadata()}", LongType, nullable = false),
          StructField("ts", LongType, nullable = false),
          StructField("value", LongType, nullable = false)
        )),
          shardingColumns = Seq("ts", s"${settings.getMetadata()}"),
          pkColumns = Seq("ts", s"${settings.getMetadata()}"))
        val indexDefinition = IndexSpecification("IndexDefinition", tableDefinition, equalColumns = Seq(s"${settings.getMetadata()}"), sortColumns = Seq(SortSpecification("ts", ColumnOrder.AscendingNullsLast)), includeColumns = Seq("value"))
        val status = ctx.get.createTableWithIndex(tableDefinition, indexDefinition)
        if (!status.isDefined) {
          this.table = Some(ctx.get.getTable(tableName))
        }
        else {
          println(s"""Error Creating table ${schemaName}.${tableName} with error ${status.get.errStr}""")
        }
      }
    }
  }

  private def generateRow(event: Event): Row = {
    Row.fromSeq(Seq(event.id, event.metadataId, event.ts, event.value))
  }

  def parseJSON (message: String): Event = {
    val event = new Event()
    val parser  = factory.createParser(message)

    while (!parser.isClosed) {
      val jsonToken = parser.nextToken()
      if (JsonToken.FIELD_NAME.equals(jsonToken)) {
        val fieldName = parser.getCurrentName
        parser.nextToken()
        if (!this.tableName.isDefined && "table".equals(fieldName)) {
            val tabName = parser.getValueAsString
            println("Received Tabname = " + tabName )
            if( tabName.nonEmpty ){
                val names = tabName.split("""\.""").map(_.trim)
                if( names.length == 2 ){
                    this.schemaName = Some(names(0))
                    this.tableName = Some(names(1))
                } else if( names.length == 1 ){
                    this.schemaName = Some(ConfigurationReader.getEventUser)
                    this.tableName = Some(names(0))
                } else {
                    println("Bad tableName input")
                }
                ConfigurationReader.setEventSchemaName(this.schemaName.get)
                println("Working with Tablename = " + this.schemaName.get + "." + this.tableName.get )
            }
        }
        else if ("id".equals(fieldName)) {event.id = parser.getValueAsLong}
        else if (s"${settings.getMetadata()}".equals(fieldName)) {event.metadataId = parser.getValueAsLong}
        else if ("ts".equals(fieldName)) {event.ts = parser.getValueAsLong}
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
          writeBatch(this.table.get, this.list.iterator)
          list.clear()
        }
      }
    }
  }

  private def writeBatch(table: ResolvedTableSchema, data: Iterator[Row]): Unit = {
    val dataSeq = data.toIndexedSeq
    val start = System.currentTimeMillis()
    if (dataSeq.size > 0) {
      try {
        val future: Future[InsertResult] = ctx.get.batchInsertAsync(table, dataSeq)
        val result: InsertResult = Await.result(future, Duration.Inf)
        if (result.failed) {
          println(s"batch insert was incomplete: $result")
        }
      } catch {
        case t: Throwable =>
          printf(s"Error writing to the IBM Db2 Event Store $t")
          ctx = None
      }
      println(s"Done inserting batch in table ${table.tableName} in ${System.currentTimeMillis() - start} ms")
    }
  }
}
