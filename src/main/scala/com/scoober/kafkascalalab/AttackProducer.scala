package com.scoober.kafkascalalab

import java.util.Properties

import akka.actor.{Actor, ActorLogging, Props}
import com.scoober.kafkascalalab.AttackProducer.Shoot
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

object AttackProducer {
  def props(power: Int): Props = Props(new AttackProducer(power))

  final case class Shoot(power: Int)

}

class AttackProducer(power: Int) extends Actor with ActorLogging {

  def shoot(shootValue: Int) = {
    val props = new Properties()
    props.put("bootstrap.servers", "kafka:9092")
    props.put("client.id", "scala-lab-producer")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)

    val TOPIC = "scala-pub"

    val record = new ProducerRecord(TOPIC, "scala-shoot", s"$shootValue")
    producer.send(record)
    producer.close()
  }

  override def receive: Receive = {
    case Shoot(shootValue) => {
      shoot(shootValue)
      if (1 == 0)
        context.system.terminate()
    }
  }
}