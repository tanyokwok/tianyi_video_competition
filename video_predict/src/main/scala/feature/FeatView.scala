package feature

import java.io.PrintWriter

import data.extract.DataLoad
import org.saddle.Vec

/**
 * Created by Administrator on 2016/1/20.
 */
object FeatView {

  def slice(begin_day:Int, last_week: Array[(String, Int, Int, Int, Int)] ): Map[String, Array[(String, (Int, Int))]] ={
    last_week.filter{
      case (uid, week, day, vid, cnt ) =>
        day >= begin_day
    }.map{
      case (uid,week,day,vid,cnt) => (uid,(vid,cnt))
    }.groupBy( _._1 )
  }
  def extract(base_pt:String, source_name:String, feat_name:String, week_id:Int, interval:Int ): Unit ={
    println("FeatView")
    val feat_out = new PrintWriter(base_pt + "/" + feat_name )

    val data = DataLoad.load( base_pt + "/" + source_name ).filter{
      case ( uid, week, day, video_site, watch_count ) =>
        week < week_id && week >= (week_id - interval)
    }

    val last_week: Array[(String, Int, Int, Int, Int)] = data.filter{
      case ( uid, week, day, vid, cnt ) =>
        week == week_id - 1
    }

    val day_1 = slice(7,last_week)
    val day_3 = slice(5,last_week)
    val day_7 = slice(1,last_week)

    val candidate = data.map{
      case ( uid, week, day, video_site, watch_count ) => (uid , video_site)
    }.toSet[(String,Int)].map{ case (uid,vid) =>

      val fold_hit_1 = Array.fill[Int](10)(0)
      val fold_cnt_1 = Array.fill[Int](10)(0)
      val fold_sum_1 = Array.fill[Int](10)(0)
      val fold_hit_3 = Array.fill[Int](10)(0)
      val fold_cnt_3 = Array.fill[Int](10)(0)
      val fold_sum_3 = Array.fill[Int](10)(0)
      val fold_hit_7 = Array.fill[Int](10)(0)
      val fold_cnt_7 = Array.fill[Int](10)(0)
      val fold_sum_7 = Array.fill[Int](10)(0)

      if( day_1.contains( uid ) )
      day_1(uid).foreach{
        case (uid,(vid,cnt) ) =>
          fold_hit_1( vid - 1 ) = 1
          fold_cnt_1( vid - 1 ) += 1
          fold_sum_1( vid - 1 ) += cnt
      }
      if( day_3.contains( uid ) )
      day_3(uid).foreach{
        case (uid,(vid,cnt) ) =>
          fold_hit_3( vid - 1 ) = 1
          fold_cnt_3( vid - 1 ) += 1
          fold_sum_3( vid - 1 ) += cnt
      }

      if( day_7.contains( uid ) )
        day_7(uid).foreach{
          case (uid,(vid,cnt) ) =>
            fold_hit_7( vid - 1 ) = 1
            fold_cnt_7( vid - 1 ) += 1
            fold_sum_7( vid - 1 ) += cnt
        }

      val lists = scala.collection.mutable.MutableList[Array[Int]]()

      lists += fold_hit_1
      lists += fold_hit_3
      lists += fold_hit_7
      lists += fold_cnt_1
      lists += fold_cnt_3
      lists += fold_cnt_7
      lists += fold_sum_1
      lists += fold_sum_3
      lists += fold_sum_7

      Range( 0,7 ).map{
        r =>
          ( uid, vid, r,lists )
      }

    }.flatMap( x => x).foreach{
      case ( uid, vid, r,lists ) =>
        feat_out.print( s"$uid\t$vid\t$r")

        lists.foreach{
          case fold =>
            fold.foreach{
              x => feat_out.print(s"\t$x")
            }
        }
        feat_out.println()
    }
    feat_out.close()
  }

  def main(args: Array[String]): Unit ={
    extract( args(0), args(1), args(2), args(3).toInt, args(4).toInt )
  }
}
