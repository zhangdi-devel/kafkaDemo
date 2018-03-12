name := "option1"

version := "0.1"

scalaVersion := "2.12.4"

libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "3.0.5" % "test",
  "org.slf4j" % "slf4j-api" % "1.7.25",
  "org.slf4j" % "slf4j-log4j12" % "1.7.25",
  "org.apache.kafka" %% "kafka" % "1.0.1",
  "org.apache.kafka" % "kafka-streams" % "1.0.1",
  "com.datastax.cassandra" % "cassandra-driver-core" % "3.4.0",
  "org.json4s" %% "json4s-native" % "3.6.0-M2",
  "org.typelevel" %% "spire" % "0.14.1"
)
