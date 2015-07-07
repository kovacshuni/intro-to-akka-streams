package com.hunorkovacs.introtoakkastreams

import akka.actor.{Inbox, ActorSystem, ActorRef}
import com.hunorkovacs.introtoakkastreams.Influx.Metric

abstract class Measured(private val influx: ActorRef, private val system: ActorSystem) {

  protected val inbox = Inbox.create(system)
}

class SlowingConsumer(name: String, influx: ActorRef, system: ActorSystem) extends Measured(influx, system) {

  private var t = 100

  def consume(i: Int) = {
    Thread.sleep(t)
    if (t < 600) t += 1
    val now = System.currentTimeMillis
    inbox.send(influx, Metric(s"$name value=$i $now", now))
  }
}

class Consumer(name: String, influx: ActorRef, system: ActorSystem) extends Measured(influx, system) {

  def consume(i: Int) = {
    Thread.sleep(2000)
    val now = System.currentTimeMillis
    inbox.send(influx, Metric(s"$name value=$i $now", now))
  }
}

class Producer(influx: ActorRef, system: ActorSystem) extends Measured(influx, system) {

  def produce() = {
    Thread.sleep(50)
    val i = 0
    val now = System.currentTimeMillis
    inbox.send(influx, Metric(s"producer value=$i $now", now))
    i
  }
}
