package com.hunorkovacs.introtoakkastreams

import akka.actor.{Props, ActorSystem}
import akka.http.scaladsl.Http
import akka.stream.{OverflowStrategy, ActorMaterializer}
import akka.stream.scaladsl.{FlowGraph, Sink, Source}

import scala.concurrent.Await
import scala.io.StdIn
import scala.concurrent.duration._

object Intro extends App {

  implicit private val sys = ActorSystem("intro")
  implicit private val mat = ActorMaterializer()
  private val influx = sys.actorOf(Props(classOf[Influx]), "influx")

  val producer = new Producer(influx, sys)
  val consumer = new Consumer(influx, sys)

  Source[Int](() => Iterator.continually(producer.produce()))
    .buffer(50, OverflowStrategy.backpressure)
    .runForeach(consumer.consume(_))

  StdIn.readLine()
  Await.ready(Http().shutdownAllConnectionPools(), 2 seconds)
  sys.shutdown()
  sys.awaitTermination(2 seconds)
}
