package stat

import data.extract.DataLoad
import org.saddle.Vec

/**
 * Created by Administrator on 2016/1/19.
 */
object VideoWeekStat {
  def extract(base_pt:String, source_name:String ): Unit ={
    println(this.getClass.getName)

    //获得每周的所有用户行为
    val action_list= DataLoad.load( base_pt + "/" + source_name ).map{
      case ( uid, week, day, video_site, watch_count ) => (  video_site, week, day, watch_count )
    }.filter{
      case (vid, week,day, count ) =>
        week > 5
    }.groupBy(_._1)

    action_list.map{
      x =>
        val vid = x._1
        val counts = Array.fill[Int](7)(0)
        val sums = Array.fill[Int](7)(0)
        val sum2 = Array.fill[Int](7)(0)

        x._2.map{
          case ( vid,week, day, watch_count) =>
            counts(day-1) += 1
            sums(day-1) += watch_count
            sum2( day - 1 ) += (if( watch_count > 10 ) 10 else watch_count)
        }
        print( vid + ":")
        counts.foreach( x => print( "\t" + x ))
        println()
        sums.foreach( x => print( "\t" + x ))
        println()
        sum2.foreach( x => print( "\t" + x ))
        println()
    }
  }

  def main(args: Array[String]): Unit ={
    extract( "E:/video_click/data/tianyi_bd_history_new", "online_train")
    //    extract( "E:/video_click/data/tianyi_bd_history_new", "online_train", "online_sample.train", 7 , 5)
  }
}
