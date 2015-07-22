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
  private val influx = sys.actorOf(Props(classOf[Influx]), "influx")

  val producer = new Producer(influx, sys)

  val pool = Http().cachedHostConnectionPool[Int](host = "localhost", port = 9000)

  Source[Int](() => Iterator.continually(producer.produce()))
    .map(i => HttpRequest(uri = "/consume", method = POST , entity = i.toString) -> i)
    .via(pool)
    .runWith(Sink.ignore)

  StdIn.readLine()
  Await.ready(Http().shutdownAllConnectionPools(), 2 seconds)
  sys.shutdown()
  sys.awaitTermination(2 seconds)
}
