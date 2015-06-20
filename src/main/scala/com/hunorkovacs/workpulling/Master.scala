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

  case class WorkWithResult[T, R](work: T, result: Try[R])

  case object TooBusy

  case class RegisterWorker(worker: ActorRef)

  case object RefreshNrOfWorkers

  def props[T, R](resultCollector: ActorRef,
                  workerType: Class[_ <: Worker[T, R]],
                  nWorkers: Int,
                  workBuffer: WorkBuffer[T]) = Props(classOf[Master[T, R]], resultCollector, workerType, nWorkers, workBuffer)
}

class Master[T, R](private val resultCollector: ActorRef,
                   private val workerType: Class[_ <: Worker[T, R]],
                   private val nWorkers: Int,
                   private val workBuffer: WorkBuffer[T]) extends Actor {

  private val logger = LoggerFactory.getLogger(getClass)
  private val workers = mutable.Set.empty[ActorRef]

  override def preStart() {
    if (logger.isDebugEnabled)
      logger.debug(s"${self.path} - Sending a message to itself to set up workers as a start...")
    self ! RefreshNrOfWorkers
  }

  override def receive = {
    case work: Work[T] =>
      if (logger.isDebugEnabled)
        logger.debug(s"${self.path} - Received work unit with hashcode ${work.hashCode}.")
      if (workBuffer.add(work)) {
        if (logger.isDebugEnabled)
          logger.debug(s"${self.path} - Work unit with hashcode ${work.hashCode} added to queue.")
        if (workers.isEmpty)
          if (logger.isWarnEnabled)
            logger.warn(s"${self.path} - There are no workers registered but work is coming in.")
        if (logger.isDebugEnabled)
          logger.debug(s"${self.path} - Sending notice to all workers that there is work to do.")
        workers foreach (_ ! WorkAvailable)
      } else {
        if (logger.isInfoEnabled)
          logger.info(s"${self.path} - Cannot handle more work because the queue is full. Sending answer: TooBusy...")
        sender ! TooBusy
      }

    case workResult: WorkWithResult[T, R] =>
      if (logger.isDebugEnabled)
        logger.debug(s"${self.path} - Received result from worker ${sender().path} with " +
          s"hashcode ${workResult.result.hashCode} for the work unit with " +
          s"hashcode ${workResult.work.hashCode}. Forwarding to collector.")
      resultCollector ! workResult

    case GiveMeWork =>
      if (logger.isDebugEnabled)
        logger.debug(s"${self.path} - ${sender().path} asked for work.")
      if (!workBuffer.isEmpty) {
        workBuffer.poll.foreach { work =>
          if (logger.isDebugEnabled)
            logger.debug(s"${self.path} - Sending work with hashcode ${work.work.hashCode} to ${sender().path}...")
          sender() ! work
        }
      }

    case RegisterWorker(worker) =>
      if (logger.isDebugEnabled)
        logger.debug(s"${self.path} - Registering worker ${worker.path}...")
      context.watch(worker)
      workers += worker

    case Terminated(worker) =>
      if (logger.isInfoEnabled)
        logger.info(s"${self.path} - Worker ${worker.path} died. Removing it from set of workers...")
      workers.remove(worker)
      self ! RefreshNrOfWorkers

    case RefreshNrOfWorkers =>
      (1 to (nWorkers - workers.size)) foreach { _ =>
        val newWorker = context.actorOf(Props(workerType, self), "pullingworker-" + randomUUID)
        if (logger.isDebugEnabled)
          logger.debug(s"${self.path} - Created new worker ${newWorker.path}. Sending itself message to register...")
        self ! RegisterWorker(newWorker)
      }
  }

  override val supervisorStrategy = SupervisorStrategy.stoppingStrategy
}
