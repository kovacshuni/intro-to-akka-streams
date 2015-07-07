package com.hunorkovacs.introtoakkastreams

import akka.actor._
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.HttpRequest
import akka.http.scaladsl.model.HttpMethods._
import akka.stream.ActorMaterializer
import akka.stream.scaladsl._

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.io.StdIn

object Intro extends App {

  implicit private val actorSystem = ActorSystem("grapher-system")
  implicit private val implicitEc = actorSystem.dispatcher
  implicit private val materializer = ActorMaterializer()
  private val influx = actorSystem.actorOf(Props(classOf[Influx]), "influx")

  val producer = new Producer(influx, actorSystem)

  val source = Source[Int](() => Iterator.continually[Int](producer.produce()))

  val poolClientFlow = Http().cachedHostConnectionPool[Int](host = "localhost", port = 9000)

  val runnable = source
    .map(i => HttpRequest(uri = "/", method = POST, entity = i.toString) -> i)
    .via(poolClientFlow)
    .runWith(Sink.ignore)

  StdIn.readLine()
  Await.ready(Http().shutdownAllConnectionPools(), 5 seconds)
  actorSystem.shutdown()
}
