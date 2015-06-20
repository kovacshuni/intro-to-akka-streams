package com.hunorkovacs.workpulling

import com.hunorkovacs.workpulling.Worker.Work

trait WorkBuffer[T] {

  def add(w: Work[T]): Boolean

  def poll: Option[Work[T]]

  def size: Int

  def isEmpty: Boolean
}
