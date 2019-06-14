package amtf

import java.io.File

import akka.actor.Actor
import common.BaseType.StaElesMap

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.language.postfixOps
import org.joda.time.DateTime
import common.MyConfig._

import scala.util.Try
import common.MyUtils._
import station.hsd.StaHsdExt
import common.TimeTransform._

/**
  * Created by 何险峰，北京 on 16-10-8.
  * 在卫星中心数据到达时就提前为工程准备好数据
  */
class HsdActor  extends Actor{
  private val cycleMinute = 10
  //private val hsd_delayMinutes = 17
  val msg = "HsdActor"

  def receive: PartialFunction[Any, Unit] = {
    case `msg` =>
      val (ymdhm, nextMinu, minute0) = calcuDelay
      context.system.scheduler.scheduleOnce(nextMinu.minutes, self, msg)
      minute0 match {
        case 0 | 10 | 20 | 30 | 40 | 50 =>
          Try {
            clearRamDisk
            mkHsdFile(ymdhm)
          } //卫星数据下载和处理
        case _ =>
          val msg = s"卫星数据下载时间格式有误."
          logMsg(ymdhm, "1","1", "M", s"$ymdhm# $msg")
      }
  }

  def calcuDelay: (String, Int, Int) = {
    val cur = DateTime.now.minusHours(8) // 北京时转世界时
      .minusMinutes(hsd_delayMinutes) // 可用资料时
    val ymdh = cur.toString("yyyyMMddHH")
    val curMinu = cur.minuteOfHour().get
    val minute0: Int = (curMinu / 10) * 10 // 10分钟为单位
    val mmStr = if (minute0 == 0) "00" else minute0.toString
    val ymdhm = s"$ymdh$mmStr"

    val nextMinu = cycleMinute - curMinu % cycleMinute

    val msg = s"葵花卫星下载...., ${nextMinu}分钟后，启动下时次葵花卫星下载"
    logMsg(ymdhm, "1","1", "I", s"$ymdhm# $msg")

    (ymdhm, nextMinu, minute0)
  }

  def clearRamDisk: Unit ={
    val fRamdisk = new File("/tmp/ramdisk")
    fRamdisk.mkdir()
    val files = fRamdisk.listFiles
    files.foreach(f => f.delete)
  }

  def mkHsdFile(ymdhm: String): StaElesMap = {
    val nextYmdhm = nextYmdhm_minute(ymdhm,10)
    val msg = s"提前下载产生卫星格点场..."
    logMsg(ymdhm, "1","1", "I", s"$ymdhm# $msg")
    StaHsdExt(nextYmdhm)
  }
}
