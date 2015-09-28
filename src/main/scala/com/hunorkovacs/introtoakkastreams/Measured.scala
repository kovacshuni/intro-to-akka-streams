package com.hunorkovacs.introtoakkastreams

import akka.actor.{Inbox, ActorSystem, ActorRef}
import com.hunorkovacs.introtoakkastreams.Influx.Metric

abstract class Measured(influx: ActorRef, sys: ActorSystem) {

  protected val inbox = Inbox.create(sys)
}

class Producer(influx: ActorRef, sys: ActorSystem)
  extends Measured(influx, sys) {

  def produce() = {
    Thread.sleep(50)
    val i = 0
    val now = System.currentTimeMillis
    inbox.send(influx, Metric(s"producer value=$i $now", now))
    i
  }
}

trait Consumer {

  def consume(i: Int)
}

class NormalConsumer(influx: ActorRef, sys: ActorSystem)
  extends Measured(influx, sys) with Consumer {

  override def consume(i: Int) = {
    Thread.sleep(100)
    val now = System.currentTimeMillis
    inbox.send(influx, Metric(s"consumer-1 value=$i $now", now))
  }
}


class SlowingConsumer(influx: ActorRef, sys: ActorSystem)
  extends Measured(influx, sys) with Consumer {

  private var t = 100

  override def consume(i: Int) = {
    Thread.sleep(t)
    if (t < 300) t = t + 1
    val now = System.currentTimeMillis
    inbox.send(influx, Metric(s"consumer-2 value=$i $now", now))
  }
}
