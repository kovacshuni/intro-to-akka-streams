package com.hunorkovacs.introtoakkastreams

import akka.actor._
import akka.http.scaladsl.Http
import akka.stream.ActorMaterializer
import akka.stream.scaladsl._
import com.hunorkovacs.introtoakkastreams.Influx.Metric

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.io.StdIn

object Intro extends App {

  implicit private val actorSystem = ActorSystem("grapher-system")
  implicit private val implicitEc = actorSystem.dispatcher
  implicit private val materializer = ActorMaterializer()
  private val influx = actorSystem.actorOf(Props(classOf[Influx]), "influx")

  val producer = new Producer(influx, actorSystem)

  val source = Source[Int](() => Iterator.continually[Int](producer.produce()))
  val workers = List(Flow[Int].map(new SlowingConsumer("consumer-1", influx, actorSystem).consume),
    Flow[Int].map(new Consumer("consumer-2", influx, actorSystem).consume))

  val runnable = source
    .via(balancer[Int, Unit](workers))
    .runWith(Sink.ignore)

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

class SlowingConsumer(name: String, influx: ActorRef, system: ActorSystem) extends Measured(influx, system) {

  private var t = 100

  def consume(i: Int) = {
    Thread.sleep(t)
    if (t < 600) t += 1
    val now = System.currentTimeMillis
    inbox.send(influx, Metric(s"$name value=$i $now", now))
  }
}

class Consumer(name: String, influx: ActorRef, system: ActorSystem) extends Measured(influx, system) {

  def consume(i: Int) = {
    Thread.sleep(70)
    val now = System.currentTimeMillis
    inbox.send(influx, Metric(s"$name value=$i $now", now))
  }
}

class Producer(influx: ActorRef, system: ActorSystem) extends Measured(influx, system) {

  def produce() = {
    Thread.sleep(50)
    val i = 0
    val now = System.currentTimeMillis
    inbox.send(influx, Metric(s"producer value=$i $now", now))
    i
  }
}

abstract class Measured(private val influx: ActorRef, private val system: ActorSystem) {

  protected val inbox = Inbox.create(system)
}