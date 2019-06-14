package amtf

import common.MyConfig._
import akka.actor.{ActorSystem, Props}
import dict.{StaDict, StaNear}
import org.apache.spark.sql.SparkSession
/**
  * 任务调度器
  * Created by 何险峰，北京 on 16-7-2.
  */
object Scheduler {
  //prepare()
  val _system = ActorSystem()

  def main(args: Array[String]): Unit = {
//    System.setProperty("com.github.fommil.netlib.BLAS", "com.github.fommil.netlib.NativeRefBLAS,com.github.fommil.netlib.F2jBLAS")
//    System.setProperty("com.github.fommil.netlib.LAPACK", "com.github.fommil.netlib.F2jLAPACK")
//    System.setProperty("com.github.fommil.netlib.ARPACK", "com.github.fommil.netlib.F2jARPACK")

    doMinute()
    if (delayMinutes < 30) {
      //hsd_ftpDir == "none" 表示没有卫星数据
      if (common.MyConfig.hsd_ftpDir != "none"){
        Thread.sleep(5 * 60 * 1000) // 运行5分钟后，可以取卫星数据
        doHsdMinute()
      }
      //rgwst_ftp == "none" 表示没有地面文件服务器数据
      /*
      if(common.MyConfig.rgwst_ftp != "none"){
        Thread.sleep(5 * 60 * 1000)
        doRgwst()
      }
      */
    }
  }
  def doMinute(): Unit ={
    val minutesActor = _system.actorOf(Props[MinutesActor], name = "MinutesActor")
    val msgMinutes = "MinutesActor"
    //if (StaDict.isLoaded && StaNear.isLoaded)

    minutesActor ! msgMinutes
  }

  def doHsdMinute(): Unit ={
    val hsd = _system.actorOf(Props[HsdActor], name = "HsdActor")
    val msgHsdMinutes = "HsdActor"
    hsd ! msgHsdMinutes
  }
  
  def doRgwst(): Unit ={
    val rgwst = _system.actorOf(Props[RgwstActor], name = "RgwstActor")
    val msgRgwst = "RgwstActor"
    rgwst ! msgRgwst
  }
}
