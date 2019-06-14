package dict

import common.MyConfig.{dictGridExtFnm, dictGridFnm}
import dict.knnGen.StaKNNGen
import dict.knnGen.StaKNNGen.mkGridKnnFile

/**
  * Created by 何险峰，维也纳 on 2017/5/10.
  */
object Init {
  def main(args: Array[String]): Unit = {
    if (args.length<3){
      println("java -calsspath calf.jar dict.Init & new 201707010000 201707050000")
    }
    val mode = args(0)
    val fromYmdhm =args(1)
    val toYmdhm =args(2)
    if (mode == "new")
      StaDictGenNew.mkDicktsByHours(fromYmdhm,toYmdhm)
    else
      StaDictGen.mkDicktsByHours(fromYmdhm,toYmdhm)
    GridDictGen.mkGridDict()
    StaKNNGen.mkOtherKnnFile()
    mkGridKnnFile(dictGridFnm)
    mkGridKnnFile(dictGridExtFnm)
  }

}
