package common
import java.io.File
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
object SparkCont {
  val c: Config = ConfigFactory.parseFile(new File("application.conf"))
  val slice = 8
  val sparkConf: Config = c.getConfig("spark")
  val appNm: String = sparkConf.getString("appNm")
  val master: String = sparkConf.getString("master")
  val cores = getCores(master).toString
  val numRepeat:Int=sparkConf.getInt("numRepeat")
  val maxbin:Int=sparkConf.getInt("maxbin")
  val numtrees:Int=sparkConf.getInt("numtrees")
  val MinInstancesPerNode:Int=sparkConf.getInt("MinInstancesPerNode")
  val MaxDepth:Int=sparkConf.getInt("MaxDepth")
  val MaxMemoryInMB:Int=sparkConf.getInt("MaxMemoryInMB")
  val CheckpointInterval:Int=sparkConf.getInt("CheckpointInterval")

  Logger.getLogger("org").setLevel(Level.ERROR)
  Logger.getLogger("akka").setLevel(Level.ERROR)
  //System.setProperty("bigdl.localMode", "true")
  //System.setProperty("bigdl.coreNumber", cores)

  val conf = new SparkConf()
    .setAppName(appNm)
    .setMaster(master)
    .set("spark.task.maxFailures", "1")
    .set("spark.local.dir","/tmp/sparktmp/")
    .set("spark.cores.max", cores)
    .set("spark.executor.cores", cores)
    .set("spark.executor.instances", cores)
    .set("spark.default.parallelism",s"$slice")
    .set("spark.kryo.unsafe","true")
    .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    //.set("spark.scheduler.mode", "FAIR")
    //.set("spark.scheduler.allocation.file", "./doc/pool.xml")


  var ss : SparkSession = null

  def sparkReset: Unit ={
    println("Spark 复位")
    if (ss != null) {
      ss.close()
    }
    ss = SparkSession.builder
      .config(conf)
      .getOrCreate()
  }

  def getCores(master:String):Int={
    val ns = master.replaceAll("[^0-9]","")
    val cores = ns.toInt
    cores
  }

  def main(args: Array[String]): Unit = {
    val c = getCores(master)
    print(c)
  }
}
