package com.hunorkovacs.introtoakkastreams

import akka.actor.{Inbox, ActorSystem, ActorRef}
import com.hunorkovacs.introtoakkastreams.Influx.Metric

abstract class Measured(influx: ActorRef, sys: ActorSystem) {

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

class SlowingConsumer(name: String, influx: ActorRef, sys: ActorSystem) extends Measured(influx, sys) with Consumer {

  private var t = 90

  override def consume(i: Int) = {
    Thread.sleep(t)
    t += 3
    val now = System.currentTimeMillis
    inbox.send(influx, Metric(s"$name value=$i $now", now))
  }
}

class NormalConsumer(name: String, influx: ActorRef, sys: ActorSystem) extends Measured(influx, sys) with Consumer {

  override def consume(i: Int) = {
    Thread.sleep(90)
    val now = System.currentTimeMillis
    inbox.send(influx, Metric(s"$name value=$i $now", now))
  }
}
