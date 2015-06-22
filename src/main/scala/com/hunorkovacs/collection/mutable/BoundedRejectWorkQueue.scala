package com.hunorkovacs.collection.mutable

import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicInteger

import com.hunorkovacs.workpulling.WorkBuffer
import com.hunorkovacs.workpulling.Worker.WorkFrom


class BoundedRejectWorkQueue[T](private val limit: Int,
                                private val refreshPeriod: Int) extends WorkBuffer[T] {

  private val queue = new ConcurrentLinkedQueue[WorkFrom[T]]()
  private val sizeCounter = new AtomicInteger(0)
  private val operationCounter = new AtomicInteger(0)

  override def add(w: WorkFrom[T]) = {
    refreshSize()
    if (sizeCounter.get() < limit) {
      sizeCounter.incrementAndGet()
      queue.add(w)
    } else
      false
  }

  def poll = {
    refreshSize()
    val tail = Option(queue.poll())
    if (tail.isDefined)
      sizeCounter.decrementAndGet()
    tail
  }

  def size = sizeCounter.get()

  def isEmpty = queue.isEmpty

  private def refreshSize() = {
    if (operationCounter.getAndIncrement() > refreshPeriod) {
      sizeCounter.set(queue.size)
      operationCounter.set(0)
    }
  }
}

object BoundedRejectWorkQueue {

  def apply[T](limit: Int, refreshPeriod: Int = 100) =
    new BoundedRejectWorkQueue[T](limit, refreshPeriod)
}
