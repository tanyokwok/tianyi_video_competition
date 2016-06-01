package stat

import java.io.PrintWriter

import data.extract.DataLoad
import org.saddle.Vec

/**
 * Created by Administrator on 2016/1/13.
 */
object DataStat {
  def extract(base_pt:String, source_name:String ): Unit ={
    println(DataStat.getClass.getName)

    //获得每周的所有用户行为
    val action_list: Map[Int, Array[(Int, String, Int, Int, Int)]] = DataLoad.load( base_pt + "/" + source_name ).map{
      case ( uid, week, day, video_site, watch_count ) => ( week, uid, day, video_site, watch_count )
    }.groupBy(_._1)

    action_list.map{
      case ( week, samples ) =>
        val counts: Array[Int] = samples.map{
          case (week, uid, day, vid, count) =>
            count
        }

        val vec = Vec( counts )
        val odd_cnt = counts.filter{
          case count =>
          count > 10
        }.size

        println(s"week $week => min = ${vec.min}, max = ${vec.max}, mean = ${vec.mean}" +
          s", median = ${vec.median}, odd = ${ odd_cnt }")
    }
  }

  def main(args: Array[String]): Unit ={
    extract( "E:/video_click/data/tianyi_bd_history_new", "online_train")
//    extract( "E:/video_click/data/tianyi_bd_history_new", "online_train", "online_sample.train", 7 , 5)
  }
}
