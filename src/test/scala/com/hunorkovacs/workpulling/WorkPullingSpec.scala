package com.hunorkovacs.workpulling

import akka.actor._
import com.hunorkovacs.collection.mutable.BoundedRejectWorkQueue
import com.hunorkovacs.workpulling.Master.WorkWithResult
import com.hunorkovacs.workpulling.Worker.Work
import org.slf4j.LoggerFactory
import org.specs2.mutable.Specification

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Success, Try}

import scala.concurrent.duration._

class WorkPullingSpec extends Specification {

  private val logger = LoggerFactory.getLogger(getClass)

  private val system = ActorSystem("test-actor-system")

  "Working" should {
    "work." in {
      logger.info("First test.")
      val n = 10
      val collector = system.actorOf(Props(classOf[Collector[Int, Int]]), "collector")
      val inbox = Inbox.create(system)
      inbox.send(collector, Collector.RegisterReady(n))
      val master = system.actorOf(Master.props[Int, Int](collector, classOf[SleepWorker], 1, BoundedRejectWorkQueue[Int](n)), "master")

      (1 to n).foreach(i => master ! Work(i))

      inbox.receive(2 seconds) must beEqualTo((1 to n).foldLeft(Set[Try[Int]]())((s, i) => s + Success(i)))
    }
  }
}

object Collector {
  case class RegisterReady(n: Int)
  case object AllReady
}

class Collector[T, R] extends Actor {
  import Collector._

  private var set = Set[WorkWithResult[T, R]]()
  var toNotify: ActorRef = ActorRef.noSender
  var n = 0

  override def receive = {
    case workWithResult: WorkWithResult[T, R] =>
      set += workWithResult
      if (set.size >= 10)
        toNotify ! set.toList

    case RegisterReady(m) =>
      toNotify = sender()
      n = m
  }
}

private class SleepWorker(master: ActorRef) extends Worker[Int, Int](master) {
  implicit private val ec = ExecutionContext.Implicits.global

  override def doWork(work: Int) = Future {
    //      Thread.sleep(1000)
    work
  }
}