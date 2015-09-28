package com.hunorkovacs.introtoakkastreams

import akka.actor.{Props, Inbox, ActorSystem}
import akka.http.scaladsl.Http
import akka.stream.{OverflowStrategy, ActorMaterializer}
import akka.stream.scaladsl._
import com.hunorkovacs.introtoakkastreams.Influx.Metric

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.io.StdIn

object Intro extends App {

  implicit private val sys = ActorSystem("intro")
  implicit private val mat = ActorMaterializer()
  private val influx = sys.actorOf(Props[Influx], "influx")

  private val producer = new Producer(influx, sys)
  private val consumers = List[Consumer](new NormalConsumer(influx, sys), new SlowingConsumer(influx, sys))
  private val consumerFlows = consumers.map(c => Flow[Int].map(i => c.consume(i)))
  private val balancer = balance[Int, Unit](consumerFlows)

  Source(() => Iterator.continually(producer.produce()))
    .via(balancer)
    .runWith(Sink.ignore)

  StdIn.readLine()
  Await.ready(Http().shutdownAllConnectionPools(), 2 seconds)
  sys.shutdown()
  sys.awaitTermination()

  def balance[In, Out](workers: List[Flow[In, Out, Unit]]) = {
    import akka.stream.scaladsl.FlowGraph.Implicits._

    Flow() { implicit builder =>
      val balance = builder.add(Balance[In](workers.size))
      val merge = builder.add(Merge[Out](workers.size))

      workers.foreach(balance ~> _ ~> merge)

      (balance.in, merge.out)
    }
  }
}
