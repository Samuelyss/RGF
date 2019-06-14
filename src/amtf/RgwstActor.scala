package amtf

import akka.actor.Actor

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.language.postfixOps
import org.joda.time.DateTime

import scala.util.Try
import common.MyUtils._
import common.TimeTransform._
import grid.hsd.HsdHxfGd
import nc.GridNcEleH
import station.cmiss.CimissLog

/**
  * 从文件服务器下载数据，并完成格点产品输出
  * Created by 何险峰，北京 on 16-10-9.
  */
class RgwstActor extends Actor {
  private val cycleMinute = 10
  private val rgwst_delayMinutes = 35
  //11、21...分启动cmis
  val msg = "RgwstActor"

  def receive: PartialFunction[Any, Unit] = {
    case `msg` =>
      val (ymdhm, nextMinu, minute0) = calcuDelay
      context.system.scheduler.scheduleOnce(nextMinu.minutes, self, msg)
      minute0 match {
        case 0 | 10 | 20 | 30 | 40 | 50 =>
          Try {
            val ymdhm_10 = prevYmdhm_minute(ymdhm, 10)
            //val (hsdFnm, hsd_exists1) = HsdHxfGd.chkYmdhmFileExists(ymdhm_10)
            // rgwst文件是备份临时使用，需要事先卫星数据存在
            val elehNcFileExists = GridNcEleH.chkNcExists(ymdhm,0)
            //val cimissAbsent = CimissLog.cimissAbent(ymdhm)
            if (! elehNcFileExists ) {
              println("cimiss 数据缺失，试图用rgwst文件......")
              GridNcEleH(ymdhm, "remote")
            }
          } //rgwst数据下载和处理
        case _ =>
          val msg = s"$ymdhm 时间格式有误."
          logMsg(ymdhm, "3","1", "F", s"$ymdhm# $msg")
      }
  }

  def calcuDelay: (String, Int, Int) = {
    val cur = DateTime.now.minusHours(8) // 北京时转世界时
      .minusMinutes(rgwst_delayMinutes) // 可用资料时
    val ymdh = cur.toString("yyyyMMddHH")
    val curMinu = cur.minuteOfHour().get
    val minute0: Int = (curMinu / 10) * 10 // 10分钟为单位
    val mmStr = if (minute0 == 0) "00" else minute0.toString
    val ymdhm = s"$ymdh$mmStr"

    val nextMinu = cycleMinute - curMinu % cycleMinute

    val msg = s"备用地面自动站文件服务方式...., ${nextMinu}分钟后，启动下时次备用."
    logMsg(ymdhm, "3","1", "I", s"$ymdhm# $msg")
    (ymdhm, nextMinu, minute0)
  }
}
