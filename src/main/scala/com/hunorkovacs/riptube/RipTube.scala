package com.hunorkovacs.riptube

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives._
import akka.stream.ActorFlowMaterializer
import org.slf4j.LoggerFactory

import scala.concurrent.Future
import scala.io.StdIn
import scala.sys.process._

object RipTube extends App {

  private val logger = LoggerFactory.getLogger(getClass)

  implicit private val system = ActorSystem("riptube-system")
  import system.dispatcher
  implicit val materializer = ActorFlowMaterializer()

//  val route =
//    path("hello") {
//      get {
//        entity(as[String]) { pageUrl =>
//          complete {
//            obtainBinaryUrl(pageUrl) flatMap { binaryUrl =>
//              downloadBinary(binaryUrl)
//            } flatMap { binary =>
//              uploadBinary(binary)
//            } map { response =>
//              StatusCode()
//            }
//              HttpResponse(StatusCodes.Created)
//          }
//        }
//      }
//    }

//  val requestHandler: HttpRequest => Future[HttpResponse] = {
//    case HttpRequest(HttpMethods.PUT, Uri.Path("/rip"), _, entity, _) =>
//      Future {
//
//      }
//
//    case _: HttpRequest =>
//      Future(HttpResponse(StatusCodes.NotFound))
//  }

//  val bindingFuture = Http().bindAndHandleAsync(route, "127.0.0.1", 8080)
  logger.info("Server online at http://localhost:8080 . Press return to stop...")
  StdIn.readLine()
//  bindingFuture
//    .flatMap(_.unbind())
//    .onComplete(_ ⇒ system.shutdown())

  private def obtainBinaryUrl(pageUrl: String) = Future {
    val raw = "youtube-dl -f bestaudio -g --skip-download " + pageUrl !!;
    raw.replaceAll("""(?m)\s+$""", "")
  }

//  private def downloadBinary(binaryUrl: String) = {
//    pipeline(pipelining.Get(binaryUrl)) map { response =>
//      response.entity.data.toByteArray
//    }
//  }
//
//  private def uploadBinary(binary: Array[Byte]) = {
//    //    HttpRequest(HttpMethods.PUT, "/dummy", entity binary)
//    pipeline(pipelining.Put("/dummy", binary))
//    //    system.actorOf(Props(new Streamer(pipeline client, 20)))
//  }
}
