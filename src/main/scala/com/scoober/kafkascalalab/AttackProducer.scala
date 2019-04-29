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

  override def receive: Receive = {
    case Shoot() =>
      shoot()

    case Shutdown() =>
      context.system.terminate()
  }

  def shoot() = {
    //TODO move the producer create and destroy to specific methods and messages
    val props = new Properties()
    props.put("bootstrap.servers", "kafka:9092")
    props.put("client.id", "scala-lab-producer")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)

    val TOPIC = "scala-pub"

    val record = new ProducerRecord(TOPIC, "scala-shoot", s"${Random.nextInt(10)}")
    producer.send(record)
    producer.close()
  }
}