package com.hunorkovacs.introtoakkastreams

import akka.actor.{Inbox, ActorSystem, ActorRef}
import com.hunorkovacs.introtoakkastreams.Influx.Metric

abstract class Measured(private val influx: ActorRef, private val sys: ActorSystem) {

  protected val inbox = Inbox.create(sys)
}

trait Consumer {

  def consume(i: Int): Unit
}

class NormalConsumer(influx: ActorRef, sys: ActorSystem) extends Measured(influx, sys) with Consumer {

  override def consume(i: Int) = {
    Thread.sleep(80)
    val now = System.currentTimeMillis
    inbox.send(influx, Metric(s"consumer-1 value=$i $now", now))
  }
}

class SlowingConsumer(influx: ActorRef, sys: ActorSystem) extends Measured(influx, sys) with Consumer {

  private var t = 100

  override def consume(i: Int) = {
    Thread.sleep(t)
    if (t < 300) t += 2
    val now = System.currentTimeMillis
    inbox.send(influx, Metric(s"consumer-2 value=$i $now", now))
  }
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
