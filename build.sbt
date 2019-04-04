name := "KafkaScalaLab"

version := "0.1"

scalaVersion := "2.12.8"

lazy val akkaVersion = "2.5.20"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor" % akkaVersion,
  "com.typesafe.akka" %% "akka-testkit" % akkaVersion,
  "org.scalactic" %% "scalactic" % "3.0.5",
  "org.scalatest" %% "scalatest" % "3.0.5" % "test",
  "org.apache.kafka" % "kafka_2.12" % "2.2.0",
  "ch.qos.logback" % "logback-classic" % "1.2.3",
  "ch.qos.logback" % "logback-core" % "1.2.3",
  "org.slf4j" % "slf4j-api" % "1.7.26"
)