package com.hunorkovacs.introtoakkastreams

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{HttpRequest, HttpMethods}
import akka.http.scaladsl.model.HttpMethods.POST
import akka.stream.{OverflowStrategy, ActorMaterializer}
import akka.stream.scaladsl._

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.io.StdIn

object Intro extends App {

  implicit private val sys = ActorSystem("intro")
  implicit private val mat = ActorMaterializer()
  private val influx = sys.actorOf(Influx.props, "influx")

  val producer = new Producer(influx, sys)
  val pool = Http().cachedHostConnectionPool[Int]("localhost", 8080)

  Source(() => Iterator.continually[Int](producer.produce()))
    .map(i => HttpRequest(method = POST, uri = "/consume", entity = i.toString) -> i)
    .via(pool)
    .runWith(Sink.ignore)

  StdIn.readLine()
  Await.ready(Http().shutdownAllConnectionPools(), 2 seconds)
  sys.shutdown()
  sys.awaitTermination()
}
