package com.scoober.kafkascalalab

import java.util.Properties

import akka.actor.{Actor, ActorLogging, Props}
import org.apache.kafka.clients.consumer.{Consumer, ConsumerConfig, KafkaConsumer}

object AttackConsumer {
  def props: Props = Props(new AttackConsumer())
}

class AttackConsumer extends Actor  with ActorLogging {
  def absorve(): Unit = {
    val properties: Properties = new Properties()
    properties.put("group.id", "elixir-pub-consumer")
    properties.put("bootstrap.servers", "localhost:9092")
    properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    properties.put("enable.auto.commit", "true")
    properties.put("auto.commit.interval.ms", "5000")

    val consumer : KafkaConsumer[String, String] = new KafkaConsumer[String, String](properties)
    val topics: List[String] = List[String]("elixir-pub")

    consumer.subscribe(topics)
  }
  override def receive: Receive = ???
}
