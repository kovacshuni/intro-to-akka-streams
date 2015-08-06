package com.hunorkovacs.introtoakkastreams

import akka.actor.{ActorSystem, Props}
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.HttpMethods.POST
import akka.http.scaladsl.model.HttpRequest
import akka.stream.ActorMaterializer
import akka.stream.scaladsl._

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.io.StdIn

object Intro extends App {

  implicit private val sys = ActorSystem("intro")
  implicit private val mat = ActorMaterializer()

  private val influx = sys.actorOf(Props[Influx], "influx")
  private val producer = new Producer(influx, sys)

  private val httpFlow = Http().cachedHostConnectionPool[Int]("localhost", 8080)

  Source(() => Iterator.continually[Int](producer.produce()))
    .map(i => HttpRequest(method = POST, uri = "/consume", entity = i.toString) -> i)
    .via(httpFlow)
    .runWith(Sink.ignore)

  StdIn.readLine()
  Await.ready(Http().shutdownAllConnectionPools(), 2 seconds)
  sys.shutdown()
  sys.awaitTermination()
}
