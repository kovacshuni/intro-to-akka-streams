package com.hunorkovacs.workpulling

import java.util.concurrent.TimeoutException

import akka.actor._
import com.hunorkovacs.collection.mutable.BoundedRejectWorkQueue
import com.hunorkovacs.workpulling.Halp.Kept
import com.hunorkovacs.workpulling.Master.Result
import com.hunorkovacs.workpulling.Master.TooBusy
import com.hunorkovacs.workpulling.Worker.WorkFrom
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
      val inbox = Inbox.create(system)
      val master = system.actorOf(PromiseKeeperMaster.props(1, BoundedRejectWorkQueue[Promise[Int]](n)), "master-1")
      val worksAndCompletions = (1 to n).toList.map(i => (WorkFrom(Promise[Int]()), Success(i)))

      worksAndCompletions.foreach(wc => inbox.send(master, wc._1))
      worksAndCompletions.foreach(wc => wc._1.work.complete(wc._2))

      val expectedResults = worksAndCompletions.map(wc => wc._1.resolveWith(wc._2))
      val actualResults = (1 to n).toList.map(_ => inbox.receive(2 seconds).asInstanceOf[Kept])
      actualResults must containTheSameElementsAs(expectedResults, (a: Kept, b: Kept) => Halp.fieldsEqual(a, b))
      actualResults.size must beEqualTo(n)
    }
    "flow nicely with n workers." in {
      val n = 10
      val inbox = Inbox.create(system)
      val master = system.actorOf(PromiseKeeperMaster.props(n, BoundedRejectWorkQueue[Promise[Int]](n)), "master-2")
      val worksAndCompletions = (1 to n).toList.map(i => (WorkFrom(Promise[Int]()), Success(i)))

      worksAndCompletions.foreach(wc => inbox.send(master, wc._1))
      worksAndCompletions.foreach(wc => wc._1.work.complete(wc._2))

      val expectedResults = worksAndCompletions.map(wc => wc._1.resolveWith(wc._2))
      val actualResults = (1 to n).toList.map(_ => inbox.receive(2 seconds).asInstanceOf[Kept])
      actualResults must containTheSameElementsAs(expectedResults, (a: Kept, b: Kept) => Halp.fieldsEqual(a, b))
      actualResults.size must beEqualTo(n)
    }
    "not flow with 0 workers." in {
      val n = 10
      val inbox = Inbox.create(system)
      val master = system.actorOf(PromiseKeeperMaster.props(0, BoundedRejectWorkQueue[Promise[Int]](n)), "master-3")

      val works = (1 to n).toList.map(w => WorkFrom(Promise.successful(w)))
      works.foreach(wc => inbox.send(master, wc))

      inbox.receive(500 millis).asInstanceOf[Set[Result[Promise[Int], Int]]] must throwA[TimeoutException]
    }
    "reject more work than buffer size. But compute the rest fine." in {
      val n = 10
      val inbox = Inbox.create(system)
      val master = system.actorOf(PromiseKeeperMaster.props(n, BoundedRejectWorkQueue[Promise[Int]](n)), "master-4")
      val worksAndCompletions = (1 to 2 * n).toList.map(i => (WorkFrom(Promise[Int]()), Success(i)))

      worksAndCompletions.foreach(wc => inbox.send(master, wc._1))

      (1 to n).foreach(_ => inbox.receive(1 second) must beEqualTo(TooBusy))

      worksAndCompletions.foreach(wc => wc._1.work.complete(wc._2))

      val expectedResults = worksAndCompletions.take(n).map(wc => wc._1.resolveWith(wc._2))
      val actualResults = (1 to n).toList.map(_ => inbox.receive(2 seconds).asInstanceOf[Kept])
      actualResults must containTheSameElementsAs(expectedResults, (a: Kept, b: Kept) => Halp.fieldsEqual(a, b))
      actualResults.size must beEqualTo(n)
    }
    "flow nicely with failures." in {
      val n = 10
      val inbox = Inbox.create(system)
      val master = system.actorOf(PromiseKeeperMaster.props(n, BoundedRejectWorkQueue[Promise[Int]](n)), "master-5")
      val worksAndCompletions = (1 to n).toList.map(i => (WorkFrom(Promise[Int]()), Failure[Int](new RuntimeException(i.toString))))

      worksAndCompletions.foreach(wc => inbox.send(master, wc._1))
      worksAndCompletions.foreach(wc => wc._1.work.complete(wc._2))

      val expectedResults = worksAndCompletions.map(wc => wc._1.resolveWith(wc._2))
      val actualResults = (1 to n).toList.map(_ => inbox.receive(2 seconds).asInstanceOf[Kept])
      actualResults must containTheSameElementsAs(expectedResults, (a: Kept, b: Kept) => Halp.fieldsEqual(a, b))
      actualResults.size must beEqualTo(n)
    }
    "flow nicely with continuous work coming in and going out." in {
      val queueLength = 8
      val nWorks = 100
      val nWorkers = 3
      val inbox = Inbox.create(system)
      val master = system.actorOf(PromiseKeeperMaster.props(nWorkers, BoundedRejectWorkQueue[Promise[Int]](queueLength)), "master-6")
      val worksAndCompletions = (1 to nWorks).toList.map { i =>
        val e: (WorkFrom[Promise[Int]], Try[Int]) = i % 2 match {
          case 0 => (WorkFrom(Promise[Int]()), Success(i))
          case 1 => (WorkFrom(Promise[Int]()), Failure[Int](new RuntimeException(i.toString)))
        }
        e
      }
      val senderQueue = mutable.Queue[(WorkFrom[Promise[Int]], Try[Int])]()
      worksAndCompletions.drop(queueLength / 2).foreach(senderQueue.enqueue(_))
      val completerQueue = mutable.Queue[(WorkFrom[Promise[Int]], Try[Int])]()
      worksAndCompletions.foreach(completerQueue.enqueue(_))

      // send in some, to pre-fill the queue but not fully
      worksAndCompletions.take(queueLength / 2).foreach(wc => inbox.send(master, wc._1))

      // uniformly produce and consume
      val actualResults = mutable.Set[Kept]()
      while (senderQueue.nonEmpty) {
        val toSend = senderQueue.dequeue()
        inbox.send(master, toSend._1)
        val toComplete = completerQueue.dequeue()
        toComplete._1.work.complete(toComplete._2)
        actualResults += inbox.receive(1 second).asInstanceOf[Kept]
      }

      // take out rest from the queue
      while (completerQueue.nonEmpty) {
        val r1 = completerQueue.dequeue()
        r1._1.work.complete(r1._2)
        actualResults += inbox.receive(1 second).asInstanceOf[Kept]
      }

      val expectedResults = worksAndCompletions.map(wc => wc._1.resolveWith(wc._2))
      actualResults must containTheSameElementsAs(expectedResults, (a: Kept, b: Kept) => Halp.fieldsEqual(a, b))
      actualResults.size must beEqualTo(nWorks)
    }
  }
/*
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
        Props(classOf[PluggableMaster], inbox.getRef(), nWorkers, BoundedRejectWorkQueue[Promise[Int]](queueSize), Propses.propses),
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
  */
}

object Collector {
  case class RegisterReady(n: Int)
  case object AllReady
}

class Collector[T, R] extends Actor {
  import Collector._

  private var set = Set[Result[T, R]]()
  var toNotify: ActorRef = ActorRef.noSender
  var n = 0

  override def receive = {
    case workWithResult: Result[T, R] =>
      set += workWithResult
      if (set.size >= 10)
        toNotify ! set

    case RegisterReady(m) =>
      toNotify = sender()
      n = m
  }
}

private class PromiseKeeperMaster(nWorkers: Int, workBuffer: WorkBuffer[Int])
  extends Master[Int, Int](nWorkers, workBuffer) {

  override protected def newWorkerProps =
    Props(classOf[PromiseWorker])
}

private object PromiseKeeperMaster {
  def props(nWorkers: Int, workBuffer: WorkBuffer[Promise[Int]]) =
    Props(classOf[PromiseKeeperMaster], nWorkers, workBuffer)
}

private class PromiseWorker extends Worker[Promise[Int], Int] {
  implicit private val ec = ExecutionContext.Implicits.global

  override def doWork(work: Promise[Int]) = work.future
}

private class PluggableMaster(nWorkers: Int, workBuffer: WorkBuffer[Int], propses: () => Props)
  extends Master[Int, Int](nWorkers, workBuffer) {

  override protected def newWorkerProps = propses()
}

private object PluggableMaster {
  def props(resultCollector: ActorRef, nWorkers: Int, workBuffer: WorkBuffer[Promise[Int]]) =
    Props(classOf[PluggableMaster], nWorkers, workBuffer)
}

private class CrashingWorker extends PromiseWorker {
  override def doWork(work: Promise[Int]) =
    throw new RuntimeException("crashed")
}

private object Halp {
  private def halp[T, R](expectedResults: List[Result[T, R]], actualResults: List[Result[T, R]]) = {
    expectedResults foreach { e =>
      if ((actualResults count { a =>
        a.work.equals(e.work) &&
          a.assigners.equals(e.assigners) &&
          a.result.equals(e.result)
      }) != 1) false
    }
    true
  }

  def fieldsEqual[T, R](a: Result[T, R], b: Result[T, R]) =
    a.work == b.work &&
      a.assigners == b.assigners &&
      a.result == b.result

  type IntPromise = WorkFrom[Promise[Int]]
  type Kept = Result[Promise[Int], Int]
}
