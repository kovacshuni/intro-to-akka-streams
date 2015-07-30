package com.hunorkovacs.introtoakkastreams

import akka.actor.{ActorSystem, Props}
import akka.http.scaladsl.Http
import akka.stream.{OverflowStrategy, ActorMaterializer}
import akka.stream.scaladsl._

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.io.StdIn

object Intro extends App {

  implicit private val sys = ActorSystem("intro-system")
  implicit private val mat = ActorMaterializer()
  private val influx = sys.actorOf(Props[Influx], "influx")

  private val producer = new Producer(influx, sys)

  private val normalFlow = Flow[Int].map(new NormalConsumer(influx, sys).consume)
  private val slowingFlow = Flow[Int].buffer(100, OverflowStrategy.backpressure).map(new SlowingConsumer(influx, sys).consume)

  private val balancer = balance(List(normalFlow, slowingFlow))

  Source(() => Iterator.continually[Int](producer.produce()))
    .via(balancer)
    .to(Sink.ignore)
    .run()

  StdIn.readLine()
  Await.ready(Http().shutdownAllConnectionPools(), 4 seconds)
  sys.shutdown()
  sys.awaitTermination()

  private def balance[In, Out](workers: List[Flow[In, Out, Unit]]) = {
    import FlowGraph.Implicits._

    Flow() { implicit builder =>
      val balance = builder.add(Balance[In](workers.size))
      val merge = builder.add(Merge[Out](workers.size))

      workers.foreach(worker => balance ~> worker ~> merge)

      (balance.in, merge.out)
    }
  }
}
