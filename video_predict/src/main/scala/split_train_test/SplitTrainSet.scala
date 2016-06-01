package split_train_test

import java.io.{PrintWriter, FileInputStream, InputStream, File}

import data.extract.DataExtract

import scala.io.Source

/**
 * Created by Administrator on 2016/1/12.
 */
object SplitTrainSet {

  def split(base_pt:String, source_name:String ): Unit ={
    val week_flag = 8
    val part1_out = new PrintWriter(  base_pt + "/online_train" )
    val part2_out = new PrintWriter(  base_pt + "/part2")

    DataExtract.extract( base_pt + "/" + source_name ).foreach{
      case (uid,  week, day, video_site, watch_count ) =>
        if( week == week_flag ){
          part2_out.println( s"$uid\t$week\t$day\t$video_site\t$watch_count" )
        }else {
          part1_out.println( s"$uid\t$week\t$day\t$video_site\t$watch_count" )
        }
    }

    part1_out.close()
    part2_out.close()
  }

  def main( args: Array[String]): Unit ={
    split( "E:/video_click/data/tianyi_bd_history_new", "sort_data")

  }
}
