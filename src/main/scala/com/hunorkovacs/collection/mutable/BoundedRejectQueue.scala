package com.hunorkovacs.collection.mutable

import java.util
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicInteger

class BoundedRejectQueue[E](private val limit: Int,
                            private val refreshPeriod: Int) extends ConcurrentLinkedQueue[E]
    with util.Queue[E] with java.io.Serializable {

  private val sizeCounter = new AtomicInteger(0)
  private val operationCounter = new AtomicInteger(0)

  override def add(e: E): Boolean = {
    refreshSize()
    if (sizeCounter.get() < limit) {
      sizeCounter.incrementAndGet()
      super.add(e)
    } else
      false
  }

  def pollOption: Option[E] = {
    refreshSize()
    val tail = Option(super.poll())
    if (tail.isDefined)
      sizeCounter.decrementAndGet()
    tail
  }

  /**
   * Use this only for obtaining the size if you expect it to be a higher number, not 0.
   * Use isEmpty() for 0 condition.
   */
  override def size = sizeCounter.get()

  private def refreshSize() = {
    if (operationCounter.getAndIncrement() > refreshPeriod) {
      sizeCounter.set(super.size)
      operationCounter.set(0)
    }
  }
}

object BoundedRejectQueue {

  def apply[E](limit: Int, refreshPeriod: Int = 100) =
    new BoundedRejectQueue[E](limit, refreshPeriod)
}
