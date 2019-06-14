package amtf

import java.io.File
import akka.actor.Actor
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.language.postfixOps
import common.TimeTransform._
import common.MyConfig._
import nc.GridNcEleH
import scala.util.{Failure, Success, Try}

/**
  * 任务计划调度
  * 以分钟精度初始化启动时间 -->
  * Created by 何险峰,成都 on 2016-4-25.
  * 参见：http://doc.akka.io/docs/akka/snapshot/scala/scheduler.html#Some_examples
  */

class MinutesActor extends Actor {
  private val cycleMinute = 10
  val msg = "MinutesActor"
  val dSrc = "cmis"

  def receive: PartialFunction[Any, Unit] = {
    case `msg` =>
      val (ymdhm, nextMinu, minute0) = calcuDelay(cycleMinute)
      context.system.scheduler.scheduleOnce(nextMinu.minutes, self, msg)
      val res = Try {
        minute0 match {
          case 0 =>
            println(s"正点$ymdhm 。。。")
            GridNcEleH(ymdhm,  dSrc)
          case 10 | 20 | 30 | 40 | 50 =>
            if (is10minu){
              println(s"分钟$ymdhm。。。")
              GridNcEleH(ymdhm,  dSrc)
            }
          case _ =>
            println("请等待。。。。")
        }
      }
      res match {
        case Success(v) =>
          if (is10minu) println(s"$ymdhm 任务完成。" + v)
        case Failure(e) =>
          println(s"$ymdhm 任务失败。" )//+ e.getMessage
      }
  }

  def rmHS_H08(): Unit = {
    new File(".").list()
      .toList
      .toArray
      .filter(f => f.indexOf("HS_H08") >= 0)
      .map(fnm => new File(fnm).delete)
  }
}

