package test

import akka.actor.{ActorSystem, Actor, Props}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.language.postfixOps

object AccaTest {
  private val _system = ActorSystem("AkkaTest")
  def main(args: Array[String]) {
    val myactor = _system.actorOf(Props[MyActor], name = "MyActor")
    val msg = "Message"
    myactor ! msg
  }

}
class MyActor extends Actor {
  var i = 0
  def receive = {
    case "Message" =>
      println(s"ok${i}")
      i += 1
      context.system.scheduler.scheduleOnce(5.seconds, self, "Message")
  }
}