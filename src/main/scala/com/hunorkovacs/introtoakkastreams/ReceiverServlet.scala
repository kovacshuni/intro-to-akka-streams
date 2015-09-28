package com.hunorkovacs.introtoakkastreams

import akka.actor.{ActorSystem, Props}
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.StatusCodes.OK
import akka.http.scaladsl.model.{HttpResponse, StatusCodes, HttpMethods, HttpRequest}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl._

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.io.StdIn

object ReceiverServlet extends App {

  implicit private val sys = ActorSystem("receiver")

  import sys.dispatcher

  implicit private val mat = ActorMaterializer()
  private val influx = sys.actorOf(Props[Influx], "influx")

  private val consumer = new NormalConsumer(influx, sys)

  import akka.http.scaladsl.server.Directives._

  private val route = {
    path("consume") {
      post {
        entity(as[String]) { i =>
          complete {
            consumer.consume(i.toInt)
            HttpResponse(OK)
          }
        }
      }
    }
  }

  val binding = Http().bindAndHandle(route, "localhost", 8080)

  StdIn.readLine()
  binding.flatMap(_.unbind())
    .flatMap(_ => Http().shutdownAllConnectionPools())
    .onComplete { _ =>
    sys.shutdown()
    sys.awaitTermination()
  }
}