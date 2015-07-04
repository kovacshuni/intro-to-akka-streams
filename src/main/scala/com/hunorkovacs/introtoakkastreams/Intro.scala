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

  val slowing = new SlowingConsumer(influx)

  Source[Int](() => Iterator.continually[Int] {
    Thread.sleep(10)
    val i = 0
    influx.bufferedWrite(s"producer value=$i ${System.currentTimeMillis}")
    i
  })
    .runForeach(slowing.consume)


  StdIn.readLine()
  influx.shutdown()
  actorSystem.shutdown()
}

class SlowingConsumer(influx: Influx) {

  private var t = 50

  def consume(i: Int) = {
    Thread.sleep(t)
    t += 1
    if (t > 500) t = 500
    influx.bufferedWrite(s"consumer value=$i ${System.currentTimeMillis}")
  }
}
