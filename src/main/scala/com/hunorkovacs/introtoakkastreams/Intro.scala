package com.hunorkovacs.introtoakkastreams

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.stream.{OverflowStrategy, ActorMaterializer}
import akka.stream.scaladsl.{Sink, Source}

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.io.StdIn

object Intro extends App {

  implicit private val sys = ActorSystem("intro")
  implicit private val mat = ActorMaterializer()
  private val influx = sys.actorOf(Influx.props, "influx")

  val producer = new Producer(influx, sys)
  val consumer = new NormalConsumer(influx, sys)
  val consumer2 = new SlowingConsumer(influx, sys)

  Source(() => Iterator.continually(producer.produce()))
    .buffer(100, OverflowStrategy.backpressure)
    .runWith(Sink.foreach(consumer2.consume))

  StdIn.readLine()
  Await.ready(Http().shutdownAllConnectionPools(), 2 seconds)
  sys.shutdown()
  sys.awaitTermination()

}
