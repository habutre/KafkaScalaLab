package com.scoober.kafkascalalab

import java.time.Duration
import java.time.temporal.ChronoUnit
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
  val consumer: KafkaConsumer[String, String] = buildConsumer()

  override def receive: Receive = {
    case Shooted() =>
      shooted()

    case Shutdown() =>
      context.system.terminate()
  }

  private def shooted(): Unit = {
    val msg = consumer.poll(Duration.of(200, ChronoUnit.MILLIS))

    msg.iterator().forEachRemaining(m => {
      log.info("MESSAGE: {}", m)
      if(m.key() == "elixir-pub")
        log.info("message: {}", m)
      else
        log.info("I am not interested on my own messages: {}", m)
    })
  }

  private def buildConsumer(): KafkaConsumer[String, String] = {
    val properties: Properties = new Properties()
    properties.put("group.id", "kafka-lab-scala-consumer")
    properties.put("bootstrap.servers", "kafka:9092")
    properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.put("enable.auto.commit", "true")
    properties.put("auto.offset.reset", "latest")
    properties.put("auto.commit.interval.ms", "5000")

    val consumer: KafkaConsumer[String, String] = new KafkaConsumer(properties)
    val topics = util.Arrays.asList("attacks")

    consumer.subscribe(topics)
    consumer
  }
}
