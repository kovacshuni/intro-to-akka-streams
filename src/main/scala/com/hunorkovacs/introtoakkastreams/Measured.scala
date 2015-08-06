package com.hunorkovacs.introtoakkastreams

import akka.actor.{ActorRef, Inbox, ActorSystem}
import com.hunorkovacs.introtoakkastreams.Influx.Metric

abstract class Measured(private val influx: ActorRef, private val sys: ActorSystem) {

  protected val inbox = Inbox.create(sys)

}

class Producer(influx: ActorRef, sys: ActorSystem) extends Measured(influx, sys) {

  def produce() = {
    Thread.sleep(50)
    val i = 0
    val now = System.currentTimeMillis
    inbox.send(influx, Metric(s"producer value=$i $now", now))
    i
  }
}

class NormalConsumer(influx: ActorRef, sys: ActorSystem) extends Measured(influx, sys) {

  def consume(i: Int) = {
    Thread.sleep(100)
    val now = System.currentTimeMillis
    inbox.send(influx, Metric(s"consumer-1 value=$i $now", now))
  }
}
