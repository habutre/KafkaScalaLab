package com.scoober.kafkascalalab

import akka.actor.{Actor, ActorLogging, ActorSystem, Props}
import com.scoober.kafkascalalab.AttackProducer.Shoot

import scala.concurrent.duration.DurationInt

object Lab extends App {

  override def main(args: Array[String]): Unit = {
    print("Scala Shooter starts")

    val labActorSystem = ActorSystem("ScalaLab")

    labActorSystem.actorOf(Props(new Lab()))
  }
}

class Lab extends Actor with ActorLogging {

  import context.dispatcher

  override def preStart(): Unit = {
    val attackProducer = context.actorOf(AttackProducer.props())
    context.system.scheduler.schedule(2.second, 200.microseconds, attackProducer, Shoot())
  }

  override def receive: Receive = {
    case _ => {
      log.info("Nothing expected here")
    }
  }
}
