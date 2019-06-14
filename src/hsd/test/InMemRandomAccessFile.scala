package hsd.test

import breeze.io.RandomAccessFile

/**
 * Created by hxf on 15-10-19.
 */
class InMemRandomAccessFile(file : java.io.File, arg0 : scala.Predef.String,name:String,data:Array[Byte]) extends RandomAccessFile(file,arg0) {
  override val rafObj = if (file == null){
    new java.io.RandomAccessFile(file, arg0)
  } else {
    new ucar.unidata.io.InMemoryRandomAccessFile(name, data).asInstanceOf[java.io.RandomAccessFile]
  }
}
