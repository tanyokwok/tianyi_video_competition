package data

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * Created by Administrator on 2016/3/13.
  */
object VisitExtract {

  def load(sc:SparkContext, fp:String): RDD[(String, Int, Int, Int, Int, Int, Int)]={
    sc.textFile(fp).map( parse )
  }

  def parse(line:String): (String, Int, Int, Int, Int, Int, Int) ={
    val arr = line.split("\\s+")
    val uid = arr(0)
    val week = arr(1).charAt(1) - '0'
    val day: Int = arr(1).charAt(3) - '0'
    val hh = arr(1).substring(4,6).toInt
    val mm = arr(1).substring(6,8).toInt

    val vid = arr(2).replace('v', ' ').trim.toInt
    val cnt = arr(3).toInt

    ( uid, week, day, vid, cnt, hh, mm)
  }
}
