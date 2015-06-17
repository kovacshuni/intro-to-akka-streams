package com.hunorkovacs.collection.mutable

import java.util
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicInteger

class BoundedRejectQueue[E](private val limit: Int) extends ConcurrentLinkedQueue[E]
    with util.Queue[E] with java.io.Serializable {

  private val PeriodLength = 100

  private val sizeCounter = new AtomicInteger(0)
  private val operationCounter = new AtomicInteger(0)

  override def add(e: E): Boolean = {
    refreshSizeApprox()
    if (sizeCounter.get() < limit)
      super.add(e)
    else
      false
  }

  override def poll(): Option[E] = {
    refreshSizeApprox()
    val tail = Option(super.poll())
    if (tail.isDefined)
      sizeCounter.decrementAndGet()
    tail
  }

  /**
   * Use this only for obtaining the size if you expect it to be a higher number, not 0.
   * Use isEmpty() for 0 condition.
   */
  def sizeApprox() = sizeCounter.get()

  private def refreshSizeApprox() = {
    if (operationCounter.getAndIncrement() > PeriodLength) {
      sizeCounter.set(super.size())
      operationCounter.set(0)
    }
  }
}
