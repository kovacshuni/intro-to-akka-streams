package com.hunorkovacs.introtoakkastreams

import akka.actor.{Props, Inbox, ActorSystem}
import akka.http.scaladsl.Http
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}
import com.hunorkovacs.introtoakkastreams.Influx.Metric

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.io.StdIn

object Intro extends App {

  implicit private val sys = ActorSystem("intro")
  implicit private val mat = ActorMaterializer()

  private val influx = sys.actorOf(Props[Influx], "influx")
  private val inbox = Inbox.create(sys)

  val source = Source(() => Iterator.continually{
    Thread.sleep(50)
    val i = 0
    val now = System.currentTimeMillis
    inbox.send(influx, Metric(s"producer $i $now", now))
    i
  })
  val sink = Sink.foreach[Int] { i =>
//    Thread.sleep(100)
//    val now = System.currentTimeMillis
//    inbox.send(influx, Metric(s"consumer $i $now", now))
  }

  val runnableFlow = source.to(sink)

  runnableFlow.run()

  StdIn.readLine
  Await.ready(Http().shutdownAllConnectionPools(), 2 seconds)
  sys.shutdown()
  sys.awaitTermination()
}
