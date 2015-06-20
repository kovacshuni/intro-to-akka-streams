package com.hunorkovacs.workpulling

import java.util.concurrent.TimeoutException

import akka.actor._
import com.hunorkovacs.collection.mutable.BoundedRejectWorkQueue
import com.hunorkovacs.workpulling.Master.WorkWithResult
import com.hunorkovacs.workpulling.Worker.Work
import org.slf4j.LoggerFactory
import org.specs2.mutable.Specification

import scala.concurrent.{Promise, ExecutionContext, Future}
import scala.util.{Success, Try}

import scala.concurrent.duration._

class WorkPullingSpec extends Specification {

  private val logger = LoggerFactory.getLogger(getClass)

  private val system = ActorSystem("test-actor-system")

  "Sending work and receiving results" should {
    "flow nicely with 1 worker." in {
      val n = 10
      val collector = system.actorOf(Props(classOf[Collector[Int, Int]]), "collector-1")
      val inbox = Inbox.create(system)
      inbox.send(collector, Collector.RegisterReady(n))
      val master = system.actorOf(Master.props[Promise[Int], Int](
        collector, classOf[PromiseKeeperWorker], 1, BoundedRejectWorkQueue[Promise[Int]](n)), "master-1")

      val worksAndNumbers = (1 to n).toList.map(i => (Work(Promise.successful(i)), i))
      val works = worksAndNumbers.map(wn => wn._1)
      works.foreach(master !)

      val expectedResults = worksAndNumbers.map(wn => WorkWithResult(wn._1.work, Success(wn._2)))
      val actualResults = inbox.receive(2 seconds).asInstanceOf[Set[WorkWithResult[Promise[Int], Int]]]
      actualResults must containAllOf(expectedResults)
      actualResults.size must beEqualTo(n)
    }
    "flow nicely with n workers." in {
      val n = 10
      val collector = system.actorOf(Props(classOf[Collector[Int, Int]]), "collector-2")
      val inbox = Inbox.create(system)
      inbox.send(collector, Collector.RegisterReady(n))
      val master = system.actorOf(Master.props[Promise[Int], Int](
        collector, classOf[PromiseKeeperWorker], n, BoundedRejectWorkQueue[Promise[Int]](n)), "master-2")

      val worksAndNumbers = (1 to n).toList.map(i => (Work(Promise.successful(i)), i))
      val works = worksAndNumbers.map(wn => wn._1)
      works.foreach(master !)

      val expectedResults = worksAndNumbers.map(wn => WorkWithResult(wn._1.work, Success(wn._2)))
      val actualResults = inbox.receive(2 seconds).asInstanceOf[Set[WorkWithResult[Promise[Int], Int]]]
      actualResults must containAllOf(expectedResults)
      actualResults.size must beEqualTo(n)
    }
    "not flow with 0 workers." in {
      val n = 10
      val collector = system.actorOf(Props(classOf[Collector[Int, Int]]), "collector-3")
      val inbox = Inbox.create(system)
      inbox.send(collector, Collector.RegisterReady(n))
      val master = system.actorOf(Master.props[Promise[Int], Int](
        collector, classOf[PromiseKeeperWorker], 0, BoundedRejectWorkQueue[Promise[Int]](n)), "master-3")

      val works = (1 to n).toList.map(w => Work(Promise.successful(w)))
      works.foreach(master !)

      inbox.receive(1 seconds).asInstanceOf[Set[WorkWithResult[Promise[Int], Int]]] must throwA[TimeoutException]
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
        toNotify ! set

    case RegisterReady(m) =>
      toNotify = sender()
      n = m
  }
}

private class PromiseKeeperWorker(master: ActorRef) extends Worker[Promise[Int], Int](master) {
  implicit private val ec = ExecutionContext.Implicits.global

  override def doWork(work: Promise[Int]) = work.future
}