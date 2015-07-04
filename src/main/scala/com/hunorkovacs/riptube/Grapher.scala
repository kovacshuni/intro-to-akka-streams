package com.hunorkovacs.riptube

import akka.actor.{Inbox, Actor, ActorSystem, Props}
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.HttpMethods.POST
import akka.http.scaladsl.model.{HttpMethods, HttpResponse, HttpRequest}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Flow, Sink, Source}
import com.hunorkovacs.riptube.InfluxWriter.{Write, Shutdown}
import org.slf4j.LoggerFactory

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.io.StdIn
import scala.util.Try

object Grapher extends App {

  implicit private val actorSystem = ActorSystem("grapher-system")
  implicit private val implicitEc = actorSystem.dispatcher

  private val generator = actorSystem.actorOf(Props(classOf[InfluxWriter]))
  private val generating = actorSystem.scheduler.schedule(0 seconds, 1000 millis, generator, Write)

  StdIn.readLine()
  generating.cancel()

  val inbox = Inbox.create(actorSystem)
  inbox.send(generator, Shutdown)
  inbox.receive(2 seconds)
  actorSystem.shutdown()
}

class InfluxWriter extends Actor {

  implicit private val actorSystem = context.system
  implicit private val implicitEc = actorSystem.dispatcher
  implicit private val materializer = ActorMaterializer()

  private val logger = LoggerFactory.getLogger(getClass)

  private val random = scala.util.Random
  private val poolClientFlow = Http().cachedHostConnectionPool[String](host = "localhost", port = 8086)

  override def receive = {
    case Write =>
      val cpu = random.nextInt(100)
      val memory = random.nextInt(33) + 20
      write("cpu", cpu)
      write("memory", memory)

    case Shutdown =>
      Await.ready(Http().shutdownAllConnectionPools(), 2 seconds)
      sender() ! None
  }

  private def write(key: String, value: Int) = {
    Source.single(HttpRequest(uri = "/write?db=mydb", method = POST, entity = s"$key value=$value") -> s"$key value=$value")
      .via(poolClientFlow)
      .runWith(Sink.head)
      .onComplete { r =>
        if (logger.isDebugEnabled) logger.debug(s"$r")
      }
  }
}

object InfluxWriter {

  case object Write
  case object Shutdown
}
