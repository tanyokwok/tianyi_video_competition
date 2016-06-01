package data.extract

import scala.io.Source

object DataExtract {

  def extract(data_pt:String): Array[(String, Int, Int, Int, Int, Int, Int)] = {
    Source.fromFile( data_pt ).getLines().map{ line =>
      val arr = line.split("\\s+")
      val uid = arr(0)
      val date_str = arr(1).replace('w', ' ').replace('d',' ').trim.split( "\\s+")
      val week = date_str( 0 ).toInt
      val day = date_str(1).substring(0,1).toInt
      val hh = date_str(1).substring(1,3).toInt
      val mm = date_str(1).substring(3,5).toInt
      val vedio_site = arr(2).replace('v', ' ').trim.toInt
      val click_count = arr(3).toInt

      ( uid, week, day, vedio_site, click_count, hh, mm)
    }.toArray
  }
}


