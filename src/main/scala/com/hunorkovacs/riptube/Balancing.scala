package com.hunorkovacs.riptube

import akka.actor.{Props, Actor, ActorSystem}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl._
import com.hunorkovacs.riptube.Generator.Generate
import org.slf4j.LoggerFactory

import scala.collection.immutable.Queue
import scala.io.StdIn
import scala.concurrent.duration._

object Balancing extends App {

  private val logger = LoggerFactory.getLogger(getClass)

  implicit private val actorSystem = ActorSystem("balancer-actor-system")
  implicit private val implicitEc = actorSystem.dispatcher
  implicit private val materializer = ActorMaterializer()

  private val queue = Queue[Int]()
  private val generator = actorSystem.actorOf(Generator.props(queue))
  private val generating = actorSystem.scheduler.schedule(0 seconds, 200 millis, generator, Generate)

  private val slowFlow = Flow[Int].map(slowProcessor)
  private val balancer = balance(slowFlow, 3)

  Source[Int](queue)
    .via(balancer)
    .runForeach(e => logger.debug(s"element $e"))

  StdIn.readLine()
  actorSystem.shutdown()

  def balance[In, Out](worker: Flow[In, Out, Any], workerCount: Int): Flow[In, Out, Unit] = {
    import FlowGraph.Implicits._

    Flow() { implicit builder =>
      val balancer = builder.add(Balance[In](workerCount, waitForAllDownstreams = true))
      val merge = builder.add(Merge[Out](workerCount))

      for (_ <- 1 to workerCount) {
        // for each worker, add an edge from the balancer to the worker, then wire
        // it to the merge element
        balancer ~> worker ~> merge
      }

      (balancer.in, merge.out)
    }
  }

  private def slowProcessor(i: Int) = {
    Thread.sleep(1000)
    i * 10
  }
}

class Generator(queue: Queue[Int]) extends Actor {

  private val logger = LoggerFactory.getLogger(getClass)
  private var i = 0

  override def receive = {
    case Generate =>
      logger.debug(s"enqueuing $i...")
      queue.enqueue(i)
      i += 1
  }
}

object Generator {

  case object Generate

  def props(queue: Queue[Int]) = Props(classOf[Generator], queue)
}
