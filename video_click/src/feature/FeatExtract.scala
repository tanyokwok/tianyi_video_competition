package feature

import data.extract.DataExtract
import utils.Utils

/**
 * Created by Administrator on 2016/1/13.
 */
object FeatExtract {

  def extract(source_pt : String): Unit ={
    val data = DataExtract.extract( source_pt ).map{
      case ( uid, week, day, video_site, watch_count ) => ( uid + " site:" + video_site,(week, day, watch_count))
    }.groupBy( _._1 ).foreach{ x =>
      x._2.foreach( a => print(a +",") )
      println()
    }
  }

  def main(args: Array[String]): Unit ={
    extract( "E:/video_click/data/tianyi_bd_history_new/sort_data")
  }

}
