package amtf
import akka.actor.{ActorSystem, Props}

/**
  * csv，hsd 数据补缺任务调度器
  * 待补前 10分钟数据，以外部参数方式输入
  *
  * Created by 何险峰，北京 on 2019-02-25.
  */
final case class MendFor(initMinutes : Int)
object MendScheduler {
  val _system = ActorSystem()
  def main(args: Array[String]): Unit = {
    val initMinutes = args(0).trim().toInt
    require(initMinutes % 10 == 0, "please enter 10,20,x0 ...")
    doMend(initMinutes)
  }
  def doMend(initMinutes : Int): Unit ={
    val mendActor = _system.actorOf(Props[MendActor], name = "MendActor")
    mendActor ! MendFor(initMinutes)
  }
}

