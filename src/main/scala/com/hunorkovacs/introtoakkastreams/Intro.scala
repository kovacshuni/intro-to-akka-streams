package com.hunorkovacs.introtoakkastreams

import akka.actor.ActorSystem
import akka.stream.impl.fusing.Buffer
import akka.stream.{OverflowStrategy, ActorMaterializer}
import akka.stream.scaladsl.Source

import scala.io.StdIn

object Intro extends App {

  implicit private val actorSystem = ActorSystem("grapher-system")
  implicit private val implicitEc = actorSystem.dispatcher
  implicit private val materializer = ActorMaterializer()
  private val influx = new Influx(actorSystem)

  val producer = new Producer(influx)
  val slowing = new SlowingConsumer(influx)

  Source[Int](() => Iterator.continually[Int](producer.produce()))
    .buffer(300, OverflowStrategy.backpressure)
    .runForeach(slowing.consume)

  StdIn.readLine()
  influx.shutdown()
  actorSystem.shutdown()
}

class SlowingConsumer(influx: Influx) {

  private var t = 100

  def consume(i: Int) = {
    Thread.sleep(t)
    if (t < 400) t += 1
    influx.bufferedWrite(s"consumer value=$i ${System.currentTimeMillis}")
  }
}

class Producer(influx: Influx) {

  def produce() = {
    Thread.sleep(50)
    val i = 0
    influx.bufferedWrite(s"producer value=$i ${System.currentTimeMillis}")
    i
  }
}