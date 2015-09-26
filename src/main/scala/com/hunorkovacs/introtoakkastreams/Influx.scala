package com.hunorkovacs.introtoakkastreams

import akka.actor.Actor
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.HttpMethods._
import akka.http.scaladsl.model.HttpRequest
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}
import com.hunorkovacs.introtoakkastreams.Influx.{Metric, Write}
import org.slf4j.LoggerFactory
import com.typesafe.cinnamon.akka.Tracer

import scala.collection.mutable
import scala.concurrent.duration._

class Influx extends Actor {

  private val logger = LoggerFactory.getLogger(getClass)
  implicit private val sys = context.system
  import context.dispatcher
  implicit private val mat = ActorMaterializer()
  private val poolClientFlow = Http().cachedHostConnectionPool[String](host = "localhost", port = 8086)
  private val metricsBuffer = mutable.Queue[Metric]()
  Tracer(context.system)

  Tracer(context.system).start("testingTrace") {
    context.system.scheduler.schedule(0 seconds, 1 seconds, self, Write)
  }

  override def receive = {
    case metric: Metric =>
      metricsBuffer.enqueue(metric)

    case Write =>
      val secondAgo = System.currentTimeMillis - 1000
      val lines = metricsBuffer.dequeueAll(_.time < secondAgo)
        .map(_.line)
        .mkString("\n")
      if (lines.nonEmpty) write(lines)
      Tracer(context.system).end("testingTrace")
  }

  private def write(lines: String) = {
    Source.single(HttpRequest(uri = "/write?db=introtoakkastreams&precision=ms", method = POST, entity = lines) -> lines)
      .via(poolClientFlow)
      .runWith(Sink.head)
      .onComplete { r =>
        if (logger.isDebugEnabled) logger.debug(s"$r")
      }
  }
}

object Influx {

  case class Metric(line: String, time: Long)

  case object Write
}
