package com.hunorkovacs.workpulling

import java.util.UUID.randomUUID

import akka.actor._
import com.hunorkovacs.collection.mutable.BoundedRejectQueue
import com.hunorkovacs.workpulling.Master._
import com.hunorkovacs.workpulling.Worker._
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.util.Try

object Master {

  case object GiveMeWork

  case class WorkResult[R](result: Try[R])

  case object TooBusy

  case class RegisterWorker(worker: ActorRef)

  case object RefreshNrOfWorkers

  def props[T, R](resultCollector: ActorRef,
                  workerType: Class[_ <: Worker[T, R]],
                  workQueueLimit: Int,
                  nWorkers: Int) = Props(classOf[Master[T, R]], resultCollector, workerType, workQueueLimit, nWorkers)
}

class Master[T, R](private val resultCollector: ActorRef,
                   private val workerType: Class[_ <: Worker[T, R]],
                   private val workQueueLimit: Int,
                   private val nWorkers: Int) extends Actor {

  private val logger = LoggerFactory.getLogger(getClass)
  private val workers = mutable.Set.empty[ActorRef]
  private val workQueue = BoundedRejectQueue[Work[T]](workQueueLimit)

  fillNrOfWorkers()

  override def receive = {
    case work: Work[T] =>
      if (workQueue.add(work)) {
        logger.info("{} - Work added to queue.", self.path)
        if (workers.isEmpty)
          logger.warn("There are no workers registered.")
        workers foreach (_ ! WorkAvailable)
      } else {
        logger.info("{} - Can't handle more work. Queue full.", self.path)
        sender ! TooBusy
      }

    case result: WorkResult[R] =>
      resultCollector ! result

    case GiveMeWork =>
      if (!workQueue.isEmpty) {
        workQueue.pollOption.foreach { work =>
            sender() ! work
            if (logger.isDebugEnabled)
              logger.info(s"${self.path} - Work with hashcode ${work.work.hashCode()} sent to worker ${sender().path}.")
        }
      }

    case RegisterWorker(worker) =>
      context.watch(worker)
      workers += worker
      logger.debug(s"Worker $worker registered.")

    case Terminated(worker) =>
      logger.info(s"Worker $worker died. Taking off the set of workers...")
      workers.remove(worker)
      self ! RefreshNrOfWorkers

    case RefreshNrOfWorkers =>
      fillNrOfWorkers()
  }

  override val supervisorStrategy = SupervisorStrategy.stoppingStrategy

  private def fillNrOfWorkers() = {
    (1 to (nWorkers - workers.size)) foreach { _ =>
      val name = "pullingworker-" + randomUUID
      logger.info("Creating worker {}...", name)
      val newWorker = context.actorOf(Props(workerType, self), name)
      context watch newWorker
      workers.add(newWorker)
    }
  }
}
