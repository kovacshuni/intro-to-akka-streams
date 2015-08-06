package com.hunorkovacs.introtoakkastreams

import akka.actor.{ActorSystem, Props}
import akka.http.scaladsl.Http
import akka.stream.ActorMaterializer
import akka.stream.scaladsl._

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.io.StdIn

object Intro extends App {

  implicit private val sys = ActorSystem("intro")
  implicit private val mat = ActorMaterializer()

  private val influx = sys.actorOf(Props[Influx], "influx")
  private val producer = new Producer(influx, sys)
  private val consumers = List(new NormalConsumer(influx, sys), new SlowingConsumer(influx, sys))

  private val balancer = balance[Int, Unit](consumers.map(consumer => Flow[Int].map(i => consumer.consume(i))))

  Source(() => Iterator.continually[Int](producer.produce()))
    .via(balancer)
    .runWith(Sink.ignore)

  StdIn.readLine()
  Await.ready(Http().shutdownAllConnectionPools(), 2 seconds)
  sys.shutdown()
  sys.awaitTermination()

  private def balance[In, Out](workers: List[Flow[In, Out, Unit]]) = {
    import akka.stream.scaladsl.FlowGraph.Implicits._

    Flow() { implicit builder =>
      val balance = builder.add(Balance[In](workers.size))
      val merge = builder.add(Merge[Out](workers.size))

      workers.foreach(balance ~> _ ~> merge)

      (balance.in, merge.out)
    }
  }
}
