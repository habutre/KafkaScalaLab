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
    val msg = Random.nextInt(11)
    val record = new ProducerRecord("attacks", 0, "scala-pub", s"$msg")

    producer.send(record)

    log.info("Published a shot with power {} to attacks topic", s"$msg")
  }

  private def buildKafkaProducer = {
    val props = new Properties()
    props.put("bootstrap.servers", "kafka:19092, kafka:29092, kafka:39092")
    props.put("client.id", "kafka-lab-scala-producer")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("security.protocol", "SSL")
    props.put("ssl.endpoint.identification.algorithm", "")
    props.put("ssl.truststore.location", "deploy/kafka.producer.truststore.jks")
    props.put("ssl.truststore.password", "confluent")
    props.put("ssl.keystore.location", "deploy/kafka.producer.keystore.jks")
    props.put("ssl.keystore.password", "confluent")
    props.put("ssl.key.password", "confluent")

    new KafkaProducer[String, String](props)
  }
}
