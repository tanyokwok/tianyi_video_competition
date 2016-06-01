package feature

import java.io.PrintWriter

import data.extract.DataLoad
import org.saddle.Vec

/**
  * Created by Administrator on 2016/1/16.
  */
object FeatWeekDay {
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
       case ( uid, week, day, video_site, watch_count ) => ( (uid , video_site) ,( week,day,watch_count) )
     }

     candidate.groupBy( _._1 ).map{ x =>
       //_._1 =>表示候选的标识， _._2 => Seq( (uid, vid), (week, wcount) )
       val ( uid, vid) = x._1

       //转换为每个工作日一个列表
       val day_week_counts: Map[Int, Array[(Int, (Int, Int))]] = x._2.map{
         case ( (uid, vid), (week,day, count) ) =>
           (day,( week, count) )
       }.groupBy( _._1 )

       Range( 0,7 ).map{
         r =>
           val one_hot = Array.fill[Int]( interval )(0)
           if( day_week_counts.contains( r + 1 ) ) {
             val week_counts: Array[(Int, (Int, Int))] = day_week_counts(r + 1)
             week_counts.foreach{
               case ( day,(week, count) ) =>
                 one_hot( week - begin_week ) = count
             }
           }
           (uid, vid, r, one_hot)
       }

     }.flatMap( x => x).foreach{
       case ( uid, vid, r, one_hot ) =>
         feat_out.print( s"$uid\t$vid\t$r")
         one_hot.foreach{
           x =>
            feat_out.print(s"\t$x")
         }
         feat_out.println()
     }
     feat_out.close()
   }

   def main(args: Array[String]): Unit ={
     extract( args(0), args(1), args(2), args(3).toInt, args(4).toInt )
//     extract( "E:/video_click/data/tianyi_bd_history_new","offline_train","feat_weekday.test", 7 ,5)
//     extract( "E:/video_click/data/tianyi_bd_history_new", "offline_train", "feat_weekday.train", 6,5 )
   }
 }
