package com.hunorkovacs.workpulling

import java.util.concurrent.TimeoutException

import akka.actor._
import com.hunorkovacs.collection.mutable.BoundedRejectWorkQueue
import com.hunorkovacs.workpulling.Master.WorkWithResult
import com.hunorkovacs.workpulling.Master.TooBusy
import com.hunorkovacs.workpulling.Worker.Work
import org.slf4j.LoggerFactory
import org.specs2.mutable.Specification

import scala.collection.mutable
import scala.concurrent.{Promise, ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

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
      val master = system.actorOf(PromiseKeeperMaster.props(
        collector, 1, BoundedRejectWorkQueue[Promise[Int]](n)), "master-1")

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
      val master = system.actorOf(PromiseKeeperMaster.props(
        collector, n, BoundedRejectWorkQueue[Promise[Int]](n)), "master-2")

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
      val master = system.actorOf(PromiseKeeperMaster.props(
        collector, 0, BoundedRejectWorkQueue[Promise[Int]](n)), "master-3")

      val works = (1 to n).toList.map(w => Work(Promise.successful(w)))
      works.foreach(master !)

      inbox.receive(1 seconds).asInstanceOf[Set[WorkWithResult[Promise[Int], Int]]] must throwA[TimeoutException]
    }
    "reject more work than buffer size. But compute the rest fine." in {
      val n = 10
      val collector = system.actorOf(Props(classOf[Collector[Int, Int]]), "collector-4")
      val inbox = Inbox.create(system)
      inbox.send(collector, Collector.RegisterReady(n))
      val master = system.actorOf(PromiseKeeperMaster.props(
        collector, n, BoundedRejectWorkQueue[Promise[Int]](n)), "master-4")

      val promisesAndNumbers = (1 to 2 * n).toList.map(i => (Work(Promise[Int]()), i))
      val works = promisesAndNumbers.map(wn => wn._1)
      works.foreach(inbox.send(master, _))

      (1 to n).foreach(_ => inbox.receive(1 second) must beEqualTo(TooBusy))

      promisesAndNumbers.foreach(pn => pn._1.work.success(pn._2))
      val expectedResults = promisesAndNumbers.filter(_._2 <= n).map(wn => WorkWithResult(wn._1.work, Success(wn._2)))
      val actualResults = inbox.receive(2 seconds).asInstanceOf[Set[WorkWithResult[Promise[Int], Int]]]
      actualResults must containAllOf(expectedResults)
      actualResults.size must beEqualTo(n)
    }
    "flow nicely with failures." in {
      val n = 10
      val collector = system.actorOf(Props(classOf[Collector[Int, Int]]), "collector-5")
      val inbox = Inbox.create(system)
      inbox.send(collector, Collector.RegisterReady(n))
      val master = system.actorOf(PromiseKeeperMaster.props(
        collector, n, BoundedRejectWorkQueue[Promise[Int]](n)), "master-5")

      val worksAndNumbers = (1 to n).toList.map { i =>
        val e = new RuntimeException(i.toString)
        (Work(Promise.failed(e)), e, i)
      }
      val works = worksAndNumbers.map(wn => wn._1)
      works.foreach(master !)

      val expectedResults = worksAndNumbers.map(wn => WorkWithResult(wn._1.work, Failure(wn._2)))
      val actualResults = inbox.receive(2 seconds).asInstanceOf[Set[WorkWithResult[Promise[Int], Int]]]
      actualResults must containAllOf(expectedResults)
      actualResults.size must beEqualTo(n)
    }
    "flow nicely with continuous work coming in and going out." in {
      val n = 8
      val m = 100
      val w = 3
      val inbox = Inbox.create(system)
      val master = system.actorOf(PromiseKeeperMaster.props(
        inbox.getRef(), w, BoundedRejectWorkQueue[Promise[Int]](n)), "master-6")

      val promisesAndTries = (1 to m).toList.map { i =>
        val e: (Work[Promise[Int]], Try[Int]) = i % 2 match {
          case 0 => (Work(Promise[Int]()), Success(i))
          case 1 => (Work(Promise[Int]()), Failure(new RuntimeException(i.toString)))
        }
        e
      }
      val workQueue = mutable.Queue[(Work[Promise[Int]], Try[Int])]()
      promisesAndTries.drop(n / 2).foreach(workQueue.enqueue(_))
      val completeQueue = mutable.Queue[(Work[Promise[Int]], Try[Int])]()
      promisesAndTries.foreach(completeQueue.enqueue(_))
      val actualResults = mutable.Set[WorkWithResult[Promise[Int], Int]]()

      // send in some, to pre-fill the queue but not fully
      promisesAndTries.take(n / 2).foreach(pt => inbox.send(master, pt._1))

      // uniformly produce and consume
      while (workQueue.nonEmpty) {
        val e1 = workQueue.dequeue()
        inbox.send(master, e1._1)
        val r1 = completeQueue.dequeue()
        r1._1.work.complete(r1._2)
        actualResults += inbox.receive(1 second).asInstanceOf[WorkWithResult[Promise[Int], Int]]
      }

      // take out rest from the queue
      while (completeQueue.nonEmpty) {
        val r1 = completeQueue.dequeue()
        r1._1.work.complete(r1._2)
        actualResults += inbox.receive(1 second).asInstanceOf[WorkWithResult[Promise[Int], Int]]
      }

      val expectedResults = promisesAndTries.map(pt => WorkWithResult(pt._1.work, pt._2))
      actualResults must containAllOf(expectedResults)
      actualResults.size must beEqualTo(m)
    }
  }

  "Crashing worker" should {
    "make a refresh action in master who will replace the worker." in {
      val queueSize = 8
      val nWorks = 100
      val nWorkers = 3
      val nCrashers = 5

      object Propses {
        val propsQueue = mutable.Queue[Props]()

        def propses: Props = propsQueue.dequeue()

        def setMaster(master: ActorRef) = {
          propsQueue.dequeueAll(_ => true)
          val crashingProps = (1 to nCrashers).toList.map(_ => Props(classOf[CrashingWorker], master))
          (crashingProps :+ Props(classOf[PromiseWorker], master)).foreach(propsQueue.enqueue(_))
        }
      }

      val inbox = Inbox.create(system)
      val master = system.actorOf(
        Props(classOf[PluggeableMaster], inbox.getRef(), nWorkers, BoundedRejectWorkQueue[Promise[Int]](queueSize), Propses.propses),
        "master-7"
      )
      Propses.setMaster(master)

      val promisesAndResults = (1 to nWorks).toList.map(i => (Work(Promise[Int]()), Success(i)))
      promisesAndResults.foreach(pr => pr._1.work.complete(pr._2))

      val actualResults = promisesAndResults.map { pr =>
        inbox.send(master, pr._1)
        inbox.receive(1 second).asInstanceOf[WorkWithResult[Promise[Int], Int]]
      }

      val expectedResults = promisesAndResults.map(pt => WorkWithResult(pt._1.work, pt._2))
      actualResults must containAllOf(expectedResults)
      actualResults.size must beEqualTo(nWorks)
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

private class PromiseKeeperMaster(resultCollector: ActorRef, nWorkers: Int, workBuffer: WorkBuffer[Int])
  extends Master[Int, Int](resultCollector, nWorkers, workBuffer) {

  override protected def newWorkerProps =
    Props(classOf[PromiseWorker], self)
}

private object PromiseKeeperMaster {
  def props(resultCollector: ActorRef, nWorkers: Int, workBuffer: WorkBuffer[Promise[Int]]) =
    Props(classOf[PromiseKeeperMaster], resultCollector, nWorkers, workBuffer)
}

private class PromiseWorker(master: ActorRef) extends Worker[Promise[Int], Int](master) {
  implicit private val ec = ExecutionContext.Implicits.global

  override def doWork(work: Promise[Int]) = work.future
}

private class PluggeableMaster(resultCollector: ActorRef, nWorkers: Int, workBuffer: WorkBuffer[Int],
                                    propses: () => Props)
  extends Master[Int, Int](resultCollector, nWorkers, workBuffer) {

  override protected def newWorkerProps = propses()
}

private object PluggeableMaster {
  def props(resultCollector: ActorRef, nWorkers: Int, workBuffer: WorkBuffer[Promise[Int]]) =
    Props(classOf[PluggeableMaster], resultCollector, nWorkers, workBuffer)
}

private class CrashingWorker(master: ActorRef) extends PromiseWorker(master) {
  override def doWork(work: Promise[Int]) =
    throw new RuntimeException("crashed")
}