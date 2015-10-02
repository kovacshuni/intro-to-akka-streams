package com.hunorkovacs.introtoakkastreams

import akka.actor.{ActorSystem, Props}
import akka.http.scaladsl.Http
import akka.stream.scaladsl._
import akka.stream.{ActorMaterializer, OverflowStrategy}

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.io.StdIn

object Intro extends App {

  implicit private val sys = ActorSystem("intro")
  implicit private val mat = ActorMaterializer()
  private val influx = sys.actorOf(Props[Influx], "influx")

  private val producer = new Producer(influx, sys)
  private val consumers = List(new NormalConsumer(influx, sys), new SlowingConsumer(influx, sys))
  private val consumerFlows = consumers.map(c => Flow[Int].map(i => c.consume(i)))
  private val balancer = balance(consumerFlows)

  Source(() => Iterator.continually(producer.produce))
    .via(balancer)
    .runWith(Sink.ignore)

  StdIn.readLine()
  Await.ready(Http().shutdownAllConnectionPools(), 2 seconds)
  sys.shutdown()
  sys.awaitTermination()

  def balance[In, Out](workers: List[Flow[In, Out, Unit]]) = {
    import akka.stream.scaladsl.FlowGraph.Implicits._

    Flow() { implicit builder =>
      val balancer = builder.add(Balance[In](workers.size))
      val merger = builder.add(Merge[Out](workers.size))

      workers.foreach(w => balancer ~> w ~> merger)

      (balancer.in, merger.out)
    }
  }
}







