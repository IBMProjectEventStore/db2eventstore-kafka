import Dependencies._
import sbt.Keys.libraryDependencies

name := "db2eventstore-kafka"

version := "0.1"

autoScalaLibrary := false

libraryDependencies += "org.scala-lang" % "scala-library" % scalaVersion.value % "provided"

lazy val kafkaQueue = (project in file("./kafka-queue"))
  .settings(defaultSettings:_*)
  .settings(
    libraryDependencies ++= spark.map(_.copy(configurations = Option("compile")))
  )
  .settings(libraryDependencies ++= core)
  .settings(dependencyDotFile := file("dependencies.dot"))
  .settings(
    maintainer := "cg8 <lj@us.ibm.com>",
    packageSummary := "Event Store Kafka Queue Spark assembly jar",
    packageDescription := "Event Store Kafka Queue Spark assembly jar",
    assemblyJarName in assembly := "db2eventstorekafkaqueue.jar",
    mainClass in assembly := Some("org.event.kafka.queue.EventStoreQueue"),
    assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false),
    assemblyMergeStrategy in assembly := {
      case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
      case PathList("META-INF", xs @ _*) => MergeStrategy.last
      case PathList("META-INF", "io.netty.versions.properties") => MergeStrategy.last
      case x => MergeStrategy.first
    })

lazy val dataLoad = (project in file("./data-load"))
  .settings(defaultSettings:_*)
  .settings(
    mainClass in Compile := Some("org.event.data.load.DataLoad"),
    mainClass in assembly := Some("org.event.data.load.DataLoad"),
    maintainer := "cg8 <lj@us.ibm.com>",
    packageSummary := "Event Store Data Load",
    packageDescription := "Event Store Data Load on Kafka Queue",
    assemblyJarName in assembly := "db2eventstoredataloader.jar",
    assemblyMergeStrategy in assembly := {
      case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
      case PathList("META-INF", xs @ _*) => MergeStrategy.last
      case PathList("META-INF", "io.netty.versions.properties") => MergeStrategy.last
      case x => MergeStrategy.first
    },
    libraryDependencies ++= core)
  .settings(dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-core"  % "2.6.7",
    dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-databind" % "2.6.7")
  .dependsOn(kafkaQueue)

lazy val eventStream = (project in file("./event-stream"))
  .settings(defaultSettings:_*)
  .settings(
    mainClass in Compile := Some("org.event.store.stream.EventStoreStream"),
    mainClass in assembly := Some("org.event.store.stream.EventStoreStream"),
    assemblyJarName in assembly := "db2eventstoreconnector.jar",
    maintainer := "cg8 <lj@us.ibm.com>",
    packageSummary := "Event Store Streaming Connector",
    packageDescription := "Event Store Kafka Streaming",
    assemblyMergeStrategy in assembly := {
      case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
      case PathList("META-INF", xs @ _*) => MergeStrategy.last
      case PathList("META-INF", "io.netty.versions.properties") => MergeStrategy.last
      case x => MergeStrategy.first
    },
    libraryDependencies ++= core)
  .settings(dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-core"  % "2.6.7",
    dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-databind" % "2.6.7")
  .dependsOn(kafkaQueue)

lazy val db2eventstoreKafka = (project in file("."))
  .aggregate(kafkaQueue, dataLoad, eventStream)
