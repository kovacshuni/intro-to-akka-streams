package com.hunorkovacs.introtoakkastreams

import akka.actor.{Inbox, ActorSystem, ActorRef}
import com.hunorkovacs.introtoakkastreams.Influx.Metric

abstract class Measured(influx: ActorRef, sys: ActorSystem) {

  protected val inbox = Inbox.create(sys)
}

class Producer(influx: ActorRef, sys: ActorSystem) extends Measured(influx, sys) {

  def produce() = {
    Thread.sleep(100)
    val now = System.currentTimeMillis
    val i = 0
    inbox.send(influx, Metric(s"producer value=$i $now", now))
    i
  }
}

class Consumer(influx: ActorRef, sys: ActorSystem) extends Measured(influx, sys) {

  private var t = 50

  def consume(i: Int) = {
    Thread.sleep(t)
    if (t < 200) t += 1
    val now = System.currentTimeMillis
    inbox.send(influx, Metric(s"consumer value=$i $now", now))
    i
  }
}
