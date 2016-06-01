package stat

import data.extract.DataLoad

/**
 * Created by Administrator on 2016/1/19.
 */
object UserWeekDayStat {
  def extract(base_pt:String, source_name:String ): Unit ={
    println(this.getClass.getName)

    //获得每周的所有用户行为
    val action_list= DataLoad.load( base_pt + "/" + source_name ).map{
      case ( uid, week, day, video_site, watch_count ) => (  video_site, week, day, watch_count )
    }.groupBy(_._1)

    action_list.map{
      x =>
        val vid = x._1
        val counts = Array.fill[Int](7)(0)
        val sums = Array.fill[Int](7)(0)
        x._2.map{
          case ( vid, week, day, watch_count) =>
            counts(day-1) += 1
            sums(day-1) += watch_count
        }
        print( vid + ":")
        counts.foreach( x => print( "\t" + x ))
        println()
        sums.foreach( x => print( "\t" + x ))
        println()
    }
  }

  def main(args: Array[String]): Unit ={
    extract( "E:/video_click/data/tianyi_bd_history_new", "online_train")
    //    extract( "E:/video_click/data/tianyi_bd_history_new", "online_train", "online_sample.train", 7 , 5)
  }
}
