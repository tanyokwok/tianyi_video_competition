package data

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * 用于加载数据
  *  */
object BehaviorExtract{

  def load(sc:SparkContext, fp:String): RDD[(String, Int, Int, Int, Int, String, Int)]={
    sc.textFile(fp).map( parse )
  }

  def parse(line:String): (String, Int, Int, Int, Int, String, Int) ={

      val arr = line.split("\t")
      val uid = arr(0)
      val week = arr(1).charAt(1) - '0'
      val day: Int = arr(1).charAt(3) - '0'
      val hh = arr(1).substring(4, 6).toInt
      val mm = arr(1).substring(6, 8).toInt
      val labels = arr(2)
      try {
        val cnt = arr(3).toInt
        (uid, week, day, hh, mm, labels, cnt)
      }catch{
        case ex=>
          ex.printStackTrace()
          println(line)
          (uid,week,day,hh,mm,labels,1)
      }finally{
        (uid,week,day,hh,mm,labels,1)
      }

  }

  def main(args:Array[String]): Unit ={
    parse("+rdnuJU6E24=\tw2d22130\t游戏,游戏类型,益智休闲\t1")
  }


}
