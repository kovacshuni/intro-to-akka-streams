package com.hunorkovacs.introtoakkastreams

import akka.actor.{Props, ActorSystem}
import akka.http.scaladsl.Http
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.io.StdIn

object Intro extends App {

  implicit private val sys = ActorSystem("intro")
  implicit private val mat = ActorMaterializer()

  private val influx = sys.actorOf(Props[Influx], "influx")
  private val producer = new Producer(influx, sys)

  Source(() => Iterator.continually[Int](producer.produce()))
    .runWith(Sink.ignore)

  StdIn.readLine()
  Await.ready(Http().shutdownAllConnectionPools(), 2 seconds)
  sys.shutdown()
  sys.awaitTermination()
}
