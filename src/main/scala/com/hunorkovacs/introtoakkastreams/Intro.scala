package com.hunorkovacs.introtoakkastreams

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.stream.{OverflowStrategy, ActorMaterializer}
import akka.stream.scaladsl._

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.io.StdIn

object Intro extends App {

  implicit private val actorSystem = ActorSystem("grapher-system")
  implicit private val implicitEc = actorSystem.dispatcher
  implicit private val materializer = ActorMaterializer()

  private val influx = new Influx(actorSystem)
  val producer = new Producer(influx)

  val source = Source[Int](() => Iterator.continually[Int](producer.produce()))
  val consumers = (1 to 2).toList.map(i => Flow[Int].map(new SlowingConsumer(new Influx(actorSystem), s"consumer-$i").consume))
  val consumers2 = List(Flow[Int].map(new SlowingConsumer(new Influx(actorSystem), "consumer-1").consume),
    Flow[Int].map(new Consumer(new Influx(actorSystem), s"consumer-2").consume))
  source
    .via(balancer[Int, Unit](consumers2))
    .to(Sink.ignore)
    .run()

  StdIn.readLine()
  Await.ready(Http().shutdownAllConnectionPools(), 5 seconds)
  actorSystem.shutdown()

  def balancer[In, Out](workers: List[Flow[In, Out, Unit]]) = {
    import FlowGraph.Implicits._

    Flow() { implicit builder =>
      val balancer = builder.add(Balance[In](workers.size, waitForAllDownstreams = true))
      val merge = builder.add(Merge[Out](workers.size))

      workers.foreach(balancer ~> _ ~> merge)

      (balancer.in, merge.out)
    }
  }
}

class SlowingConsumer(influx: Influx, name: String) {

  private var t = 100

  def consume(i: Int) = {
    Thread.sleep(t)
    if (t < 600) t += 1
    influx.bufferedWrite(s"$name value=$i ${System.currentTimeMillis}")
  }
}

class Consumer(influx: Influx, name: String) {

  def consume(i: Int) = {
    Thread.sleep(70)
    influx.bufferedWrite(s"$name value=$i ${System.currentTimeMillis}")
  }
}

class Producer(influx: Influx) {

  def produce() = {
    Thread.sleep(50)
    val i = 0
    influx.bufferedWrite(s"producer value=$i ${System.currentTimeMillis}")
    i
  }
}
