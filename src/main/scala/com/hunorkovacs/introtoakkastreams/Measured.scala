package com.hunorkovacs.introtoakkastreams

import akka.actor.{ActorRef, Inbox, ActorSystem}
import com.hunorkovacs.introtoakkastreams.Influx.Metric

abstract class Measured(val influx: ActorRef, sys: ActorSystem) {

  protected val inbox = Inbox.create(sys)
}

class Producer(influx: ActorRef, sys: ActorSystem) extends Measured(influx, sys) {

  def produce() = {
    Thread.sleep(50)
    val now = System.currentTimeMillis
    val i = 0
    inbox.send(influx, Metric(s"producer value=$i $now", now))
    i
  }
}

trait Consumer {
  def consume(i: Int)
}

class NormalConsumer(influx: ActorRef, sys: ActorSystem) extends Measured(influx, sys)
    with Consumer {

  override def consume(i: Int) = {
    Thread.sleep(1000)
    val i = 0
    val now = System.currentTimeMillis
    inbox.send(influx, Metric(s"consumer-1 value=$i $now", now))
  }
}

class SlowingConsumer(influx: ActorRef, sys: ActorSystem) extends Measured(influx, sys)
    with Consumer {

  private var t = 80

  override def consume(i: Int) = {
    Thread.sleep(t)
    if (t < 400) t += 1
    val i = 0
    val now = System.currentTimeMillis
    inbox.send(influx, Metric(s"consumer-2 value=$i $now", now))
  }
}
