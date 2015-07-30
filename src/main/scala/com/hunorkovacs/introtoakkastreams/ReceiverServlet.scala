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

  implicit private val sys = ActorSystem("receiver-system")
  import sys.dispatcher
  implicit private val mat = ActorMaterializer()
  private val influx = sys.actorOf(Props[Influx], "influx-receiver")

  private val consumer = new NormalConsumer(influx, sys)

  private val route = {
    path("consume") {
      post {
        entity(as[String]) { i =>
          consumer.consume(i.toInt)
          complete(HttpResponse(OK))
        }
      }
    }
  }

  private val binding = Http().bindAndHandle(route, "localhost", 8080)

  StdIn.readLine()
  binding.flatMap(_.unbind()) map { _ =>
    Await.ready(Http().shutdownAllConnectionPools(), 4 seconds)
    sys.shutdown()
    sys.awaitTermination()
  }
}
