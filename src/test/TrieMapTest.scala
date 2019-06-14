package test

import scala.collection.concurrent._
object TrieMapTest {
  val cach = new TrieMap[Int,Int]()

  def main(args: Array[String]): Unit = {
    cach += ((1,1),(2,2))
    cach +=((0,0))
    cach +=((5,5))
    cach -=(1)
    println(cach)

    val v = cach(5)
    println(v)

    println(cach.contains(5))

    println(cach.exists(f => f._1==5 && f._2==5))

    cach.getOrElseUpdate(6,6)
    println(cach)

    cach.put(7,7)
    println(cach)

    println(cach.min)

    cach.clear()
    println(cach)
  }
}
