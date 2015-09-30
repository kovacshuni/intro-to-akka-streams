package com.hunorkovacs.introtoakkastreams

import akka.actor.{Inbox, Props, ActorSystem}
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{HttpMethods, HttpRequest}
import akka.stream.{OverflowStrategy, ActorMaterializer}
import akka.stream.scaladsl._
import com.hunorkovacs.introtoakkastreams.Influx.Metric

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.io.StdIn

object Intro extends App {

  implicit private val sys = ActorSystem("intro")
  implicit private val mat = ActorMaterializer()

  private val influx = sys.actorOf(Props[Influx], "influx")

  val producer = new Producer(influx, sys)

  val connection = Http().cachedHostConnectionPool[Int]("localhost", 8080)

  Source(() => Iterator.continually(producer.produce))
    .map(i => HttpRequest(method = HttpMethods.POST, uri = "/consume", entity = i.toString) -> i)
    .via(connection)
    .runWith(Sink.ignore)

  StdIn.readLine()
  Await.ready(Http().shutdownAllConnectionPools(), 2 seconds)
  sys.shutdown()
  sys.awaitTermination()
}
