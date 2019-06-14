package amtf

import java.io.File

import akka.actor.Actor
import common.MyConfig._
import common.TimeTransform._
import grid.GdEleH._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.{Failure, Success, Try}

/**
  * 数据补缺任务计划调度
  * 以10分钟精度，对前 x0 分钟可能缺测数据补缺.
  * x0分钟以外部参数方式输入
  *
  * Created by 何险峰,北京 on 2019-3-25.
  * 参见：http://doc.akka.io/docs/akka/snapshot/scala/scheduler.html#Some_examples
  */

class MendActor extends Actor {
  private val cycleMinute = 10
  val dSrc = "cmis"

  override def receive: Receive = {
    case MendFor(initMinutes) =>
      val (ymdhm0, nextMinu, _) = calcuDelay(cycleMinute)
      context.system.scheduler.scheduleOnce(nextMinu.minutes, self, MendFor(initMinutes))
      val res = Try {
        mend(ymdhm0, initMinutes)
      }
      res match {
        case Success(v) =>
          println(s"$ymdhm0 任务完成。" + v)
        case Failure(e) =>
          println(s"$ymdhm0 任务失败。") //+ e.getMessage
      }
  }

  def mend(ymdhm0: String, initMinutes: Int): Unit = {
    val checkDMinu = Range(0,144 * 5).map(minu => minu * 10 + initMinutes)
    print(s"修补")
    checkDMinu.map { dm =>
      val prev_ymdhm = prevYmdhm_minute(ymdhm0, dm)
      print(s"$prev_ymdhm,")
      val h = getHours(prev_ymdhm)
      val m = getMinutes(prev_ymdhm)
      if (h==2 && m==40 ) {
        //0240 file not existed for hsd always
        val (csv0fnm,_) = getCsvFnms(prev_ymdhm)
        val csv0exist = new File(csv0fnm).exists()
        if (! csv0exist) apply4sta_csv0(prev_ymdhm, dSrc)
      } else apply4sta_csv0(prev_ymdhm, dSrc)
    }
    println()
  }
}


