package com.hunorkovacs.introtoakkastreams

import java.util.concurrent.atomic.AtomicLong

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.HttpMethods._
import akka.http.scaladsl.model.HttpRequest
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}
import org.slf4j.LoggerFactory

import scala.concurrent.Await
import scala.concurrent.duration._

class Influx(actorSystem: ActorSystem) {

  private val logger = LoggerFactory.getLogger(getClass)
  implicit private val implicitSystem = actorSystem
  implicit private val implicitEc = actorSystem.dispatcher
  implicit private val materializer = ActorMaterializer()
  private val poolClientFlow = Http().cachedHostConnectionPool[String](host = "localhost", port = 8086)
  private val lastTimeWritten = new AtomicLong(System.currentTimeMillis())
  private var lines = StringBuilder.newBuilder

  def write(entity: String) = {
    Source.single(HttpRequest(uri = "/write?db=introtoakkastreams&precision=ms", method = POST,
        entity = s"$entity") -> s"$entity")
      .via(poolClientFlow)
      .runWith(Sink.head)
      .onComplete { r =>
        if (logger.isDebugEnabled) logger.debug(s"$r")
      }
  }

  def bufferedWrite(line: String) = {
    val now = System.currentTimeMillis()
    if (now - lastTimeWritten.get() > 1000 && lines.nonEmpty) {
      lastTimeWritten.set(now)
      val entity = lines
        .append(line)
        .append("\n")
        .mkString
      lines.clear()
      Source.single(HttpRequest(uri = "/write?db=introtoakkastreams&precision=ms", method = POST, entity = entity) -> entity)
        .via(poolClientFlow)
        .runWith(Sink.head)
        .onComplete { r =>
          if (logger.isDebugEnabled) logger.debug(s"$r")
        }
    } else {
      lines = lines
        .append(line)
        .append("\n")
    }
  }

  def shutdown() = Await.ready(Http().shutdownAllConnectionPools(), 5 seconds)
}
