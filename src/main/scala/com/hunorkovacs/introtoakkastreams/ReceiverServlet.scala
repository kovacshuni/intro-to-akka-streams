package com.hunorkovacs.introtoakkastreams

import akka.actor.{ActorSystem, Props}
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives._
import akka.stream.ActorMaterializer
import org.slf4j.LoggerFactory

import scala.io.StdIn

object ReceiverServlet extends App {

  implicit private val actorSystem = ActorSystem("receiver-system")
  implicit private val implicitEc = actorSystem.dispatcher
  implicit private val materializer = ActorMaterializer()
  private val influx = actorSystem.actorOf(Props(classOf[Influx]), "influx")

  val consumer = new Consumer("http-consumer", influx, actorSystem)

  val route =
    path("") {
      post {
        entity(as[String]) { s =>
          complete {
            consumer.consume(s.toInt)
            HttpResponse(OK)
          }
        }
      }
    }

  val bindingFuture = Http().bindAndHandle(route, "localhost", 9000)

  StdIn.readLine()
  bindingFuture
    .flatMap(_.unbind())
    .onComplete(_ ⇒ actorSystem.shutdown())
}
