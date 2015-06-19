package com.hunorkovacs.workpulling

import akka.actor._
import com.hunorkovacs.workpulling.Master.WorkResult
import com.hunorkovacs.workpulling.Worker.Work
import org.slf4j.LoggerFactory
import org.specs2.mutable.Specification

import scala.concurrent.Future
import scala.util.{Success, Try}

import scala.concurrent.duration._

class WorkPullingSpec extends Specification {

  private val logger = LoggerFactory.getLogger(getClass)

  private val system = ActorSystem("test-actor-system")

  "Working" should {
    "work." in {
      logger.info("First test.")
      val n = 10
      val collector = system.actorOf(Props(classOf[SetCollector[Int]]), "collector")
      val inbox = Inbox.create(system)
      inbox.send(collector, SetCollector.RegisterReady(n))
      val master = system.actorOf(Master.props[Int, Int](collector, classOf[SleepWorker], n, 1), "master")

      (1 to n).foreach(i => master ! Work(i))

      inbox.receive(2 seconds) must beEqualTo((1 to n).foldLeft(Set[Try[Int]]())((s, i) => s + Success(i)))
    }
  }
}

object SetCollector {
  case class RegisterReady(n: Int)
  case object AllReady
}

class SetCollector[T] extends Actor {
  import SetCollector._

  private val logger = LoggerFactory.getLogger(getClass)

  private var set = Set[Try[T]]()
  var toNotify: ActorRef = ActorRef.noSender
  var n: Int = 0

  override def receive = {
    case t: WorkResult[T] =>
      logger.info("WorkResult received: {}", t)
      set += t.result
      if (set.size >= 10)
        toNotify ! set.toList

    case RegisterReady(m) =>
      toNotify = sender()
      n = m
  }
}

private class SleepWorker(master: ActorRef) extends Worker[Int, Int](master) {
  private val logger = LoggerFactory.getLogger(getClass)

  override def doWork(work: Int) = Future {
    //      Thread.sleep(1000)
    logger.info("Working...")
    work
  }
}