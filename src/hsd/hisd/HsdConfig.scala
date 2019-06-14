package hsd.hisd
/**
  * Created by 何险峰，北京 on 15-11-26.
  */
case class Band(n:String,cnm:String,res : String)
object HsdConfig {
  // val ramdisk ="/tmp/ramdisk"
  //val conf: Config = ConfigFactory.parseFile(new File("application.conf"))
  //val comm: Config = conf.getConfig("comm")
  //val ncDir: String = comm.getString("ncDir")
  //val hsd_ftpDir: String = comm.getString("hsd_ftpDir")
  val numSwath: Int = 5
  //val hsd_delayMinutes : Int = comm.getInt("hsd_delayMinutes")

  //val param: Config = conf.getConfig("param")
  val width: Int = 3100      //东西像元数目
  val height: Int = 1900     //南北像元数目
  val ltlon: Float = 73f     //区域左上角经度
  val ltlat: Float = 55f     //区域左上角纬度
  val dlat: Float = 0.02f    //图像像素
  val dlon: Float = 0.02f    //图像像素
  val lats: Array[Float] = ls(ltlat,ltlat - height * dlat,height)
  val lons: Array[Float] = ls(ltlon,ltlon + width * dlon,width)
  val bands: List[Band] = List[Band](
    Band("01","蓝光(0.43,0.48)μm","10"),
    Band("02","绿光(0.50,0.52)μm","10"),
    Band("03","红光(0.63,0.66)μm","05"),
    Band("04","反射(0.85,0.87)μm","10"),
    Band("05","反射(1.60,1.62)μm","20"),
    Band("06","反射(2.25,2.27)μm","20"),
    Band("07","辐射(3.74,3.96)μm","20"),
    Band("08","辐射(6.06,6.43)μm","20"),
    Band("09","辐射(6.89,7.01)μm","20"),
    Band("10","辐射(7.26,7.43)μm","20"),
    Band("11","辐射(8.44,8.76)μm","20"),
    Band("12","辐射(9.54,9.72)μm","20"),
    Band("13","辐射(10.3,10.6)μm","20"),
    Band("14","辐射(11.1,11.3)μm","20"),
    Band("15","辐射(12.2,12.5)μm","20"),
    Band("16","辐射(13.2,13.4)μm","20")
  )
  val nBand: Int = bands.length
  private def ls(start:Float,stop:Float,n:Int):Array[Float]={
    val ls = breeze.linalg.linspace(start,stop,n)
    ls.data.map(f => f.toFloat)
  }
}
