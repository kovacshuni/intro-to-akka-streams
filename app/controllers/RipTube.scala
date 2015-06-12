package controllers

import akka.actor.{Props, ActorSystem}
import play.api.mvc._
import sys.process._

import spray.client.pipelining
import spray.client.pipelining._
import spray.http._

import java.io.File

import scala.concurrent.Future

class RipTube extends Controller {

  implicit private val system = ActorSystem()
  import system.dispatcher
  private val pipeline: HttpRequest => Future[HttpResponse] = sendReceive

  def rip = Action.async { request =>
  	obtainBinaryUrl("https://www.youtube.com/watch?v=3nB9iMHSIVg") flatMap { binaryUrl =>
      downloadBinary(binaryUrl)
    } flatMap { binary =>
      uploadBinary(binary)
    } map { response =>
      Results.Status(response.status.value.toInt)
    }
  }

  def dummy = Action.async(parse.file(new File("test-download.m4a"))) { request =>
    Future(Ok("saved"))
  }

  private def obtainBinaryUrl(pageUrl: String) = Future {
    val raw = "youtube-dl -f bestaudio -g --skip-download " + pageUrl !!;
    raw.replaceAll("""(?m)\s+$""", "")
  }

  private def downloadBinary(binaryUrl: String) = {
    pipeline(pipelining.Get(binaryUrl)) map { response =>
      response.entity.data.toByteArray
    }
  }

  private def uploadBinary(binary: Array[Byte]) = {
//    HttpRequest(HttpMethods.PUT, "/dummy", entity binary)
    pipeline(pipelining.Put("/dummy", binary))
//    system.actorOf(Props(new Streamer(pipeline client, 20)))
  }
}
