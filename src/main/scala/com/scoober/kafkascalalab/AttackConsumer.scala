package com.scoober.kafkascalalab

import java.time.Duration
import java.time.temporal.ChronoUnit
import java.util
import java.util.Properties

import akka.actor.{Actor, ActorLogging, Props}
import com.scoober.kafkascalalab.AttackConsumer.{Shooted, Shutdown}
import org.apache.kafka.clients.consumer.{ConsumerRecord, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

object AttackConsumer {
  def props(): Props = Props(new AttackConsumer())

  final case class Shooted()
  final case class Shutdown()
}

class AttackConsumer extends Actor with ActorLogging {
  val consumer: KafkaConsumer[String, String] = buildConsumer()
  val producer: KafkaProducer[String, String] = buildInteractionsProducer()

  override def receive: Receive = {
    case Shooted() =>
      shooted()

    case Shutdown() =>
      context.system.terminate()
  }

  private def shooted(): Unit = {
    val msg = consumer.poll(Duration.of(200, ChronoUnit.MILLIS))

    msg.iterator().forEachRemaining(m => {
      confirmMessageAck(m.key(), m)
    })
  }

  private def confirmMessageAck(key: String, msg: ConsumerRecord[String,String]): Unit = key match {
    case "scala-pub" => log.info("I am not interested on my own messages: {}", msg)

    case "elixir-pub" => {
      val ackMsg = new ProducerRecord("interactions", 0,"scala-damage", s"${msg.value()}")
      log.info("Published an ack with power {} to interactions topic: {}", msg.value(), msg)
      producer.send(ackMsg)
    }

    case _ => log.info("Unknown message: {}", msg)
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

  private def buildInteractionsProducer() = {
    val props = new Properties()
    props.put("bootstrap.servers", "kafka:9092")
    props.put("client.id", "kafka-lab-scala-producer")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    new KafkaProducer[String, String](props)
  }
}
