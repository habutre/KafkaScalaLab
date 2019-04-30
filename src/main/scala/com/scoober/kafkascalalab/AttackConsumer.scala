package com.scoober.kafkascalalab

import java.util
import java.util.Properties

import akka.actor.{Actor, ActorLogging, Props}
import com.scoober.kafkascalalab.AttackConsumer.{Shooted, Shutdown}
import org.apache.kafka.clients.consumer.KafkaConsumer

object AttackConsumer {
  def props(): Props = Props(new AttackConsumer())

  final case class Shooted()

  final case class Shutdown()

}

class AttackConsumer extends Actor with ActorLogging {
  override def receive: Receive = {
    case Shooted() =>
      shooted()

    case Shutdown() =>
      context.system.terminate()
  }

  def shooted(): Unit = {
    val properties: Properties = new Properties()
    properties.put("group.id", "elixir-pub-consumer")
    properties.put("bootstrap.servers", "localhost:9092")
    properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    properties.put("enable.auto.commit", "true")
    properties.put("auto.commit.interval.ms", "5000")

    val consumer: KafkaConsumer[String, String] = new KafkaConsumer(properties)
    val topics = util.Arrays.asList("elixir-pub")

    consumer.subscribe(topics)
  }
}
