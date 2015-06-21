package com.hunorkovacs.workpulling

import akka.actor.{Props, Actor, ActorRef}
import com.hunorkovacs.workpulling.Master._
import com.hunorkovacs.workpulling.Worker._
import org.slf4j.LoggerFactory

import scala.concurrent.{Promise, Future}

object Worker {

  case object WorkAvailable

  case class Work[T](work: T)

  def props[T, R](master: ActorRef) = Props(classOf[Worker[T, R]], master)
}

abstract class Worker[T, R](private val master: ActorRef) extends Actor {
  private val logger = LoggerFactory.getLogger(getClass)
  implicit private val ec = context.dispatcher

  override def preStart() {
    if (logger.isDebugEnabled)
      logger.debug(s"${self.path} - Asking for work...")
    master ! GiveMeWork
  }

  override def receive = {
    case WorkAvailable =>
      if (logger.isDebugEnabled)
        logger.debug(s"${self.path} - Received notice that work is available. Asking for work...")
      master ! GiveMeWork

    case Work(work: T) =>
      if (logger.isDebugEnabled)
        logger.debug(s"${self.path} - Starting to work on work unit with hashcode ${work.hashCode}...")
      doWorkAssociated(work) onSuccess { case workWithResult =>
        if (logger.isDebugEnabled) {
          val resultHash = workWithResult.result.getOrElse(workWithResult).hashCode
          logger.debug(s"${self.path} - Sending result with hashcode $resultHash of the work unit with hashcode ${workWithResult.work.hashCode}...")
        }
        master ! workWithResult
        if (logger.isDebugEnabled)
          logger.debug(s"${self.path} - Asking for work from ${master.path}...")
        master ! GiveMeWork
      }
  }

  private def doWorkAssociated(work: T): Future[WorkWithResult[T, R]] = {
    val p = Promise[WorkWithResult[T, R]]()
    doWork(work).onComplete(c => p.success(WorkWithResult(work, c)))
    p.future
  }

  protected def doWork(work: T): Future[R]
}

