package com.scoober.kafkascalalab

import org.apache.kafka.common.utils.Scheduler

object Lab {

  import java.util.Properties

  import org.apache.kafka.clients.producer._

  def main(args: Array[String]) {
    val props = new Properties()
    props.put("bootstrap.servers", "kafka:9092")
    props.put("client.id", "scala-lab-producer")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)

    val TOPIC = "scala-pub"

    for (i <- 1 to 50) {
      val record = new ProducerRecord(TOPIC, "key", s"hello new $i")
      producer.send(record)
    }

    val record = new ProducerRecord(TOPIC, "key", "the end " + new java.util.Date)
    producer.send(record)

    producer.close()
  }
}
