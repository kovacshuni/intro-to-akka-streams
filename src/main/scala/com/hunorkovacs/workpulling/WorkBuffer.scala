package com.hunorkovacs.workpulling

import com.hunorkovacs.workpulling.Worker.WorkFrom

trait WorkBuffer[T] {

  def add(w: WorkFrom[T]): Boolean

  def poll: Option[WorkFrom[T]]

  def size: Int

  def isEmpty: Boolean
}
