package data.extract

import scala.io.Source

/**
 * Created by Administrator on 2016/1/13.
 */
object DataLoad {

  def load( source_pt : String ): Array[(String,Int,Int,Int,Int)] ={
    Source.fromFile( source_pt ).getLines().map{ line =>
      val arr = line.split("\\s+")
      val uid = arr(0)
      val week = arr( 1 ).toInt
      val day = arr(2).toInt
      val vedio_site = arr(3).toInt
      val click_count = arr(4).toInt

      ( uid, week, day, vedio_site, click_count)
    }.toArray
  }

  def main(arr: Array[String]){
    load( "E:\\video_click\\data\\tianyi_bd_history_new\\insight_2").foreach( x => println( x  ))
  }
}
