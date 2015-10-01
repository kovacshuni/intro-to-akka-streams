package com.hunorkovacs.introtoakkastreams

import akka.actor.{Inbox, Props, ActorSystem}
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

  private val producer = new Producer(influx, sys)
  private val consumer = new SlowingConsumer(influx, sys)

  Source(() => Iterator.continually(producer.produce))
    .runWith(Sink.foreach(consumer.consume))

  StdIn.readLine()
  Await.ready(Http().shutdownAllConnectionPools(), 2 seconds)
  sys.shutdown()
  sys.awaitTermination()
}





