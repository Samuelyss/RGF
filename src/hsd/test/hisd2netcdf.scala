package hsd.test

/**
 * Created by hxf on 15-9-21.
 */
trait hisd2netcdf {
  val NORMAL_END=        0
  val ERROR_ARG=         1
  val ERROR_FILE_OPEN=   2
  val ERROR_CALLOCATE=   3
  val ERROR_READ_HEADER= 4
  val ERROR_INFO=        5
  val ERROR_READ_DATA=   6
  val ERROR_PARAMETER=   7
  val ERROR_MAKE_HEADER= 8
  val ERROR_MAKE_DATA=   9
  val ERROR_WRITE=       10

  def mjd_to_date(mjd:Double, date : Array[Int] = Array.fill(7)(0)):Unit
  def DateGetNowInts(date: Array[Int] = Array.fill(7)(0) ):Unit
  def DateIntsToMjd(date: Array[Int] = Array.fill(7)(0)):Double
}
