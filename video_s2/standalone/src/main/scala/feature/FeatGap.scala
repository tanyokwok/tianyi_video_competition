package feature

import java.io.PrintWriter

import data.extract.DataLoad
import feature.FeatAvgGap.Params
import feature.FeatOnGap._
import scopt.OptionParser

import scala.collection.immutable.{Iterable, IndexedSeq}

object FeatGap extends Feature {

  def extract(base_pt:String, source_name:String, feat_name:String, week_id:Int, interval:Int ): Unit ={
    println( "FeatGap")

    val datas = DataLoad.load( base_pt + "/" + source_name ).filter{
      case ( uid, week, day, video_site, watch_count ) =>
        week < week_id && week >= (week_id - interval)
    }.map{
      case ( uid, week, day, video_site, watch_count )
      => ( (uid , video_site) ,( ( week - 1) * 7 +  day, watch_count) )
    }.groupBy( _._1 ).map {
      group =>
        val (uid, vid) = group._1
        val last_day = group._2.map(y => y._2._1).sorted.max
        ( uid, vid, last_day )
    }.groupBy( _._1 ).map{
      group =>
        val uid = group._1
        val values = toMap[Int](group._2)
        (uid, values)
    }
    val path = base_pt + "/" + feat_name
    print[Int](path,datas,
      (data,did,vid,uid) =>{
        val last_day = data(vid)
        val day = (week_id - 1) * 7 + did + 1
        val day_gap = day - last_day
        s"$did\t$vid\t$uid\t$day_gap"
      }
    )
  }

  def main(args: Array[String]): Unit ={

    val default_params = Params()
    parser.parse(args,default_params) match {
      case Some(params) => run(params)
      case None => System.exit(1)
    }

  }

}
