package com.scoober.kafkascalalab

import java.util.Properties

import akka.actor.{Actor, ActorLogging, Props}
import com.scoober.kafkascalalab.AttackProducer.{Shoot, Shutdown}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.util.Random

object AttackProducer {
  def props(): Props = Props(new AttackProducer())

  final case class Shoot()
  final case class Shutdown()
}

class AttackProducer() extends Actor with ActorLogging {
  val producer = buildKafkaProducer

  override def receive: Receive = {
    case Shoot() =>
      shoot()

    case Shutdown() =>
      producer.close()
      context.system.terminate()
  }

  private def shoot() = {
    val msg = Random.nextInt(10)
    val record = new ProducerRecord("attacks", 0, "scala-pub", s"$msg")

    log.info("Preparing to publish a record: {}", s"$msg")

    producer.send(record)

    log.info("Published a shot with power {} to attacks topic", s"$msg")
  }

  private def buildKafkaProducer = {
    val props = new Properties()
    props.put("bootstrap.servers", "kafka:9092")
    props.put("client.id", "kafka-lab-scala-producer")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    new KafkaProducer[String, String](props)
  }
}
