package feature

import java.io.PrintWriter

import data.extract.DataLoad
import org.saddle.Vec

import scala.collection.immutable.IndexedSeq

/**
 * Created by Administrator on 2016/1/16.
 */
object FeatWeek {
  def extract(base_pt:String, source_name:String, feat_name:String, week_id:Int, interval:Int ): Unit ={
    println( "FeatWeek")
    val feat_out = new PrintWriter(base_pt + "/" + feat_name )

    val begin_week = (week_id - interval)
    val end_week = week_id
    //获取候选集
    val candidate = DataLoad.load( base_pt + "/" + source_name ).filter{
      case ( uid, week, day, video_site, watch_count ) =>
        week < end_week && week >= begin_week
    }.map{
      case ( uid, week, day, video_site, watch_count ) => ( (uid , video_site) ,( week, watch_count) )
    }

    candidate.groupBy( _._1 ).map{ x =>
      //_._1 =>表示候选的标识， _._2 => Seq( (uid, vid), (week, wcount) )
      val ( uid, vid) = x._1

      //转换为每周一个列表
      val week_counts = x._2.map{
        case ( (uid, vid), (week, count) ) =>
          (week, count)
      }.groupBy( _._1 )

      val feats: Map[Int, (Int, Int, Double, Double, Double)] = week_counts.map{
        x =>
          val week = x._1

          val counts: Array[Int] = x._2.map{
            case (week,cnt) =>
              cnt
          }

          val vec = Vec( counts )
          val cnt = vec.length
          val sum = vec.sum
          val median = vec.median
          val mean = vec.mean
          val stdev = vec.stdev
          ( week,( cnt, sum, median, mean, stdev ))
      }

      val week_feats= Range(begin_week,end_week).map{
        wk =>
          if( !feats.contains(wk ) ) (0,0,0,0,0,0)
          else{
            val x = feats(wk)
            (1, x._1,x._2,x._3,x._4,x._5)
          }
      }
      Range( 0,7 ).map{
        r =>
          ( uid, vid, r, week_feats )
      }

    }.flatMap( x => x).foreach{
      case ( uid, vid, r, week_feats ) =>
        feat_out.print( s"$uid\t$vid\t$r")
        week_feats.foreach{
          case (f1,f2,f3,f4,f5,f6) =>
            feat_out.print(s"\t$f1\t$f2\t$f3\t$f4\t$f5\t$f6")
//            feat_out.print(s"\t$f1")
        }
        feat_out.println()
    }
    feat_out.close()
  }

  def main(args: Array[String]): Unit ={
    extract( args(0), args(1), args(2), args(3).toInt, args(4).toInt )
//    extract( "E:/video_click/data/tianyi_bd_history_new","offline_train","feat_week1.test", 7 ,5)
//    extract( "E:/video_click/data/tianyi_bd_history_new", "offline_train", "feat_week1.train", 6,5 )
//    extract( "E:/video_click/data/tianyi_bd_history_new","online_train","online/feat_week.test", 8 ,5)
//    extract( "E:/video_click/data/tianyi_bd_history_new", "online_train", "online/feat_week.train", 7,5 )

  }
}
