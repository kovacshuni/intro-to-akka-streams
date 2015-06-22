package com.hunorkovacs.workpulling

import java.util.UUID.randomUUID

import akka.actor._
import com.hunorkovacs.workpulling.Master._
import com.hunorkovacs.workpulling.Worker._
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.util.Try

object Master {

  case object GiveMeWork

  class Result[W, R] private (val work: W, val assigners: List[ActorRef], val result: Try[R]) {
    
    def popAssigner() = new Result(work, assigners.tail, result)
  }

  object Result {
    def apply[W, R](work: W, result: Try[R]) = new Result(work, Nil, result)

    def apply[W, R](workFrom: WorkFrom[W], result: Try[R]) = new Result(workFrom.work, workFrom.assigners, result)
  }

  case object TooBusy
}

abstract class Master[T, R](private val nWorkers: Int,
                            private val workBuffer: WorkBuffer[T]) extends Actor {

  private val logger = LoggerFactory.getLogger(getClass)
  private val workers = mutable.Set.empty[ActorRef]

  override def preStart() =
    refreshNrOfWorkers()

  override def receive = {
    case work: WorkFrom[T] =>
      if (workBuffer.add(work.assignedBy(sender()))) {
        if (logger.isDebugEnabled)
          logger.debug(s"${self.path} - Work unit with hashcode ${work.work.hashCode} added to queue. Sending notice to all workers...")
        if (workers.isEmpty)
          if (logger.isWarnEnabled)
            logger.warn(s"${self.path} - There are no workers registered but work is coming in.")
        workers foreach (_ ! WorkAvailable)
      } else {
        if (logger.isInfoEnabled)
          logger.info(s"${self.path} - Received work unit ${work.work.hashCode} but queue is full. TooBusy!")
        sender ! TooBusy
      }

    case result: Result[T, R] =>
      val returnTo = result.assigners.head
      if (logger.isDebugEnabled) {
        val resultHash = result.result.getOrElse(result).hashCode
        logger.debug(s"${self.path} - Received result from ${sender().path} " +
          s"with result hashcode $resultHash " +
          s"of work unit hashcode ${result.work.hashCode}. Returning to ${returnTo.path}...")
      }
      returnTo ! result.popAssigner()

    case GiveMeWork =>
      if (logger.isDebugEnabled)
        logger.debug(s"${self.path} - Worker asked for work: ${sender().path}")
      if (!workBuffer.isEmpty) {
        workBuffer.poll.foreach { work =>
          if (logger.isDebugEnabled)
            logger.debug(s"${self.path} - Sending work with hashcode ${work.work.hashCode} to ${sender().path}...")
          sender() ! work
        }
      }

    case Terminated(worker) =>
      if (logger.isInfoEnabled)
        logger.info(s"${self.path} - Termination message got from and restarting worker ${worker.path}...")
      workers.remove(worker)
      context.unwatch(worker)
      refreshNrOfWorkers()
  }

  private def refreshNrOfWorkers() = {
    while (workers.size < nWorkers) {
      val newWorker = context.actorOf(newWorkerProps, "pullingworker-" + randomUUID)
      context.watch(newWorker)
      workers += newWorker
      if (logger.isDebugEnabled)
        logger.debug(s"${self.path} - Created and watching new worker ${newWorker.path}...")
    }
  }

  protected def newWorkerProps: Props

  override val supervisorStrategy = SupervisorStrategy.stoppingStrategy
}
