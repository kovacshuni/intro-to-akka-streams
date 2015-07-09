package com.hunorkovacs.introtoakkastreams

import akka.actor.{Props, ActorSystem}
import akka.http.scaladsl.Http
import akka.stream.{OverflowStrategy, ActorMaterializer}
import akka.stream.scaladsl._

import scala.collection.parallel.Splitter
import scala.concurrent.Await
import scala.io.StdIn
import scala.concurrent.duration._

object Intro extends App {

  implicit private val sys = ActorSystem("intro")
  implicit private val mat = ActorMaterializer()
  private val influx = sys.actorOf(Props(classOf[Influx]), "influx")

  val producer = new Producer(influx, sys)
  val consumers = List(new SlowingConsumer(s"consumer-1", influx, sys),
    new NormalConsumer(s"consumer-2", influx, sys)
  )

  val realSinks = consumers.map(c => Flow[Int].map(c.consume))
  val balancedSinks = balancer(realSinks)

  Source[Int](() => Iterator.continually(producer.produce()))
    .via(balancedSinks)
    .runWith(Sink.ignore)

  StdIn.readLine()
  Await.ready(Http().shutdownAllConnectionPools(), 2 seconds)
  sys.shutdown()
  sys.awaitTermination(2 seconds)

  def balancer[In, Out](workers: List[Flow[In, Out, Unit]]) = {
    import FlowGraph.Implicits._

    Flow() { implicit builder =>
      val broadcast = builder.add(Broadcast[In](workers.size))
      val merge = builder.add(Merge[Out](workers.size))

      workers.foreach(broadcast ~> _ ~> merge)

      (broadcast.in, merge.out)
    }
  }



  def balancerrrr[In, Out](workers: List[Flow[In, Out, Unit]]) = {
    import FlowGraph.Implicits._

    Flow() { implicit builder =>
      val balancer = builder.add(Balance[In](workers.size, waitForAllDownstreams = true))
      val merge = builder.add(Merge[Out](workers.size))

      workers.foreach(balancer ~> _ ~> merge)

      (balancer.in, merge.out)
    }
  }
}
