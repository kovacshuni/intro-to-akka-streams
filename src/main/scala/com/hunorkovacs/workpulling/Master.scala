package com.hunorkovacs.workpulling

import akka.actor._
import com.hunorkovacs.collection.mutable.BoundedRejectQueue
import com.hunorkovacs.workpulling.Master._
import com.hunorkovacs.workpulling.Worker._
import org.slf4j.LoggerFactory

import scala.collection.mutable

object Master {

  case object GiveMeWork

  case object TooBusy

  case class RegisterWorker(worker: ActorRef)

  case object RefreshNrOfWorkers

}

class Master[T, R](private val workLimit: Int, private val workerLimit: Int) extends Actor {

  private val logger = LoggerFactory.getLogger(getClass)
  private val workers = mutable.Set.empty[ActorRef]
  private val workQueue = BoundedRejectQueue[Work[T]](workLimit)

  override def receive = {
    case work: Work[T] =>
      if (workQueue.add(work)) {
        if (workers.isEmpty)
          logger.warn("Got work but there are no workers registered.")
        workers foreach (_ ! WorkAvailable)
      } else
        sender ! TooBusy

    case GiveMeWork =>
      if (!workQueue.isEmpty) {
        val work = workQueue.pollOption
        if (work.isDefined)
          sender() ! work
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
      (1 to (workerLimit - workers.size)) foreach { _ =>
        val newWorker = context.actorOf(Props[Worker[T, R]])
        context watch newWorker
        workers.add(newWorker)
      }
  }

  override val supervisorStrategy = SupervisorStrategy.stoppingStrategy
}
