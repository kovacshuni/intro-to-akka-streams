package com.hunorkovacs.workpulling

import akka.actor.{Props, Actor, ActorRef}
import com.hunorkovacs.workpulling.Master._
import com.hunorkovacs.workpulling.Worker._

import scala.concurrent.Future

object Worker {

  case object WorkAvailable

  case class Work[T](work: T)

  def props[T, R](master: ActorRef) = Props(classOf[Worker[T, R]], master)
}

abstract class Worker[T, R](private val master: ActorRef) extends Actor {
  implicit val ec = context.dispatcher

  override def preStart() {
    master ! RegisterWorker(self)
    master ! GiveMeWork
  }

  override def receive = {
    case WorkAvailable =>
      master ! GiveMeWork

    case Work(work: T) =>
      doWork(work) onComplete { result =>
        master ! WorkResult(result)
        master ! GiveMeWork
      }
  }

  def doWork(work: T): Future[R]
}
