package test

object Entities {
  case class Aa(a: Int, b: Int)
  case class Bb(b: Int, a: Int)
  val as = Seq( Aa(1,3), Aa(2,4)  )
  val bs = Seq( Bb(5,3), Bb(6,4)  )
}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
object DFUnionTest {
  val sconf = new SparkConf()
    .setAppName("Test")
    .setMaster("local")
    .set("spark.local.dir","/tmp/sparktmp/")

  val spark = SparkSession.builder.config(sconf).getOrCreate()
  val sc = spark.sparkContext

  def main(args: Array[String]): Unit = {
    un
  }

  def un={
    import spark.implicits._
    val aDF = sc.parallelize(Entities.as, 2).toDF()
    val bDF = sc.parallelize(Entities.bs, 2).toDF()
    aDF.show()
    bDF.show()
    aDF.union(bDF).show
  }
}
