package com.hunorkovacs.riptube

import akka.actor.Actor
import akka.actor.Actor.Receive
import com.hunorkovacs.riptube.DownloadCoordinator.Download

import scala.collection.immutable.Queue

object DownloadCoordinator {

  case class Download(binaryUrl: String)
}

class DownloadCoordinator extends Actor {

  private val queue = Queue[Download]()

  override def receive = {
    case d: Download =>
      queue.enqueue(d)
  }
}
