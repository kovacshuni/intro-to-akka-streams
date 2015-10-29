package com.hunorkovacs.introtoakkastreams

import akka.actor.{ActorSystem, Props}
import akka.stream.scaladsl._
import akka.stream.{ActorMaterializer, OverflowStrategy}

import scala.io.StdIn

object Intro extends App {

  implicit private var sys = ActorSystem("intro")
  implicit private var mat = ActorMaterializer()

  private val influx = sys.actorOf(Props[Influx])

  private val producer = new Producer(influx, sys)

  Source(() => Iterator.continually(producer.produce))
    .buffer(100, OverflowStrategy.backpressure)
    .runWith(Sink.ignore)

  StdIn.readLine()
  sys.shutdown()
  sys.awaitTermination()
}
