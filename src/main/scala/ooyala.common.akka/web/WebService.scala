package ooyala.common.akka.web

import akka.actor.{ActorSystem, Actor, ActorRef, Props}
import spray.routing.{Route, HttpService}

import spray.can.Http
import akka.io.IO
import akka.actor.ActorLogging


/**
 * Contains methods for starting and stopping an embedded Spray web server.
 */
object WebService {
  case object Stop

  var service: ActorRef = _

  class ServiceActor(route: Route) extends Actor with HttpService  {
    
    println("Createing class instance")
    
    def handler: Receive = {
      case Stop => context.system.shutdown()
    }

    // the HttpService trait defines only one abstract member, which
    // connects the services environment to the enclosing actor or test
    def actorRefFactory = context

    def receive = runRoute(route) orElse handler
  }

  /**
   * Starts a web server given a Route.  Note that this call is meant to be made from an App or other top
   * level scope, and not within an actor, as system.actorOf may block.
   *
   * @param route The spray Route for the service.  Multiple routes can be combined like (route1 ~ route2).
   * @param system the ActorSystem to use
   * @param host The host string to bind to, defaults to "0.0.0.0"
   * @param port The port number to bind to
   */
  def startServer(route: Route, system: ActorSystem,
                  host: String = "0.0.0.0", port: Int = 8090) {
    implicit val system = ActorSystem()

    service = system.actorOf(Props(new ServiceActor(route)), "sprayService")
    //val ioBridge = IOExtension(system).ioBridge
   // val server = system.actorOf(Props(new HttpServer(ioBridge, SingletonHandler(service))), "httpServer")
    //server ! HttpServer.Bind(host, port)
    println("Binding on Http Port")
    IO(Http) ! Http.Bind(service, interface = host, port = port)
  }

  def stopServer() { service ! Stop }
}
