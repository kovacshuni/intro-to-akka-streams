package com.hunorkovacs.introtoakkastreams

import akka.actor.{Inbox, Props, ActorSystem}
import akka.stream.{OverflowStrategy, ActorMaterializer}
import akka.stream.scaladsl._
import com.hunorkovacs.introtoakkastreams.Influx.Metric

import scala.concurrent.Future
import scala.io.StdIn

object Intro extends App {

  implicit private var sys = ActorSystem("intro")
  implicit private var mat = ActorMaterializer()

  private val influx = sys.actorOf(Props[Influx])

  private val producer = new Producer(influx, sys)
  private val consumers = List(new NormalConsumer(influx, sys))
  private val consumerFlow = consumers.map(c => Flow[Int].map(i => c.consume(i)))
  private val balancer = balance(consumerFlow)

  Source(() => Iterator.continually(producer.produce))
    .buffer(100, OverflowStrategy.backpressure)
    .via(balancer)
    .runWith(Sink.ignore)

  StdIn.readLine()
  sys.shutdown()
  sys.awaitTermination()


  val v1 = Future()
  v2 = Future(b(v88))
  r = Future(c(v2))



}
