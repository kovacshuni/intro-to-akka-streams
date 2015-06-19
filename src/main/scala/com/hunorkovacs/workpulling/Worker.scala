package com.hunorkovacs.workpulling

import akka.actor.{Actor, ActorRef}
import com.hunorkovacs.workpulling.Master._
import com.hunorkovacs.workpulling.Worker._

import scala.concurrent.Future

object Worker {

  case object WorkAvailable

  case class Work[T](work: T)

}

abstract class Worker[T, R](private val master: ActorRef)(implicit manifest: Manifest[T]) extends Actor {
  implicit val ec = context.dispatcher

  override def preStart() {
    master ! RegisterWorker(self)
    master ! GiveMeWork
  }

  override def receive = {
    case WorkAvailable =>
      master ! GiveMeWork

    case Work(work: T) =>
      doWork(work) onComplete { t =>
        master ! t
        master ! GiveMeWork
      }
  }

  def doWork(work: T): Future[R]
}
