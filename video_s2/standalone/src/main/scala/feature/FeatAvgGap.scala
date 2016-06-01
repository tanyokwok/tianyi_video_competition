package feature

import java.io.PrintWriter

import data.extract.DataLoad
import data.transform.DataTransform.Params
import feature.FeatOnGap._
import org.saddle.Vec
import scopt.OptionParser

import scala.collection.immutable.{Iterable, IndexedSeq}

object FeatAvgGap extends Feature{

  def calculate(group: ((String, Int), Array[((String, Int), (Int, Int))]),
                week_id:Int ):(String, Int, Double) ={
    val ( uid, vid) = group._1
    val visit_days = group._2.map( y => y._2._1 ).sorted

    val day = ( week_id - 1 ) * 7 + 1
    val left = Vec(  ( visit_days.toList:+ day ).toArray )
    val right = Vec( ( 0::visit_days.toList).toArray )

    val avg_gap = (left - right ).mean

    (uid, vid, avg_gap)

  }

  def extract(base_pt:String, source_name:String, feat_name:String, week_id:Int, interval:Int ): Unit ={

    val datas: Map[String, Map[Int, Double]] = DataLoad.load( base_pt + "/" + source_name ).filter{
      case ( uid, week, day, video_site, watch_count ) =>
        week < week_id && week >= (week_id - interval)
    }.map{
      case ( uid, week, day, video_site, watch_count )
      => ( (uid , video_site) ,( ( week - 1) * 7 +  day, watch_count) )
    }.groupBy( _._1 )
      .map{
        group =>
        calculate(group,week_id)
    }.groupBy( _._1 ).map{
      group =>
        val uid = group._1
        val values = toMap[Double](group._2)
        (uid, values)
    }

    val path = base_pt + "/" + feat_name
    print[Double](
      path, datas, (data,did,vid,uid) =>{
        val avg_gap: Double = data(vid)
        s"$did\t$vid\t$uid\t$avg_gap"
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
