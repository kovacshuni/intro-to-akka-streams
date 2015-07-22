package com.hunorkovacs.introtoakkastreams

import akka.actor.{ActorSystem, Props}
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.model.StatusCodes.OK
import akka.http.scaladsl.server.Directives._
import akka.stream.ActorMaterializer

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.io.StdIn

object ReceiverServlet extends App {

  implicit private val sys = ActorSystem("receiver")
  import sys.dispatcher
  implicit private val mat = ActorMaterializer()
  private val influx = sys.actorOf(Props(classOf[Influx]), "influx")

  val consumer = new NormalConsumer("consumer", influx, sys)

  val route =
    path("consume") {
      post {
        entity(as[String]) { s =>
          complete {
            println("what")
            consumer.consume(s.toInt)
            HttpResponse(OK)
          }
        }
      }
    }

  val binding = Http().bindAndHandle(route, "localhost", 9000)

  StdIn.readLine()
  binding
    .flatMap(_.unbind())
    .onComplete { _ =>
      Await.ready(Http().shutdownAllConnectionPools(), 2 seconds)
      sys.shutdown()
      sys.awaitTermination(2 seconds)
    }
}
