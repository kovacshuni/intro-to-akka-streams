package com.hunorkovacs.introtoakkastreams

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Keep, Flow, Sink, Source}
import org.slf4j.LoggerFactory

import scala.io.StdIn

object Intro extends App {

  implicit private val actorSystem = ActorSystem("grapher-system")
  implicit private val implicitEc = actorSystem.dispatcher
  implicit private val materializer = ActorMaterializer()
  private val influx = new Influx(actorSystem)

  val source = Source[Int](() => Iterator.continually[Int] {
    Thread.sleep(43)
    val i = 0
    influx.bufferedWrite(s"producer value=$i ${System.currentTimeMillis}")
    i
  })
  val sink = Sink.foreach[Int](i => influx.bufferedWrite(s"consumer value=$i ${System.currentTimeMillis}"))
  val runnableFlow = source.toMat(sink)(Keep.right)

  runnableFlow.run()

  StdIn.readLine()
  influx.shutdown()
  actorSystem.shutdown()
}
