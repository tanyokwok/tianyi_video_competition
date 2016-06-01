package feature

import java.io.PrintWriter

import data.extract.DataLoad
import feature.FeatAvgGap._
import feature.FeatOHEID.Params
import org.saddle.Vec
import scopt.OptionParser

/**
  * Created by Administrator on 2016/1/16.
  */
object FeatOnGap extends Feature{


  def caculate(group: ((String, Int), Array[((String, Int), (Int, Int))]),
               week_id: Int) = {
    val ( uid, vid) = group._1
    val visit_days = group._2.map( y => y._2._1 ).sorted
    val day = ( week_id - 1 ) * 7 + 1
    val left = Vec(  ( visit_days.toList:+ day ).toArray )
    val right = Vec( ( 0::visit_days.toList).toArray )

    val avg_gap = (left - right ).mean

    val last_day = group._2.map( y => y._2._1 ).sorted.max
    (uid,vid,(avg_gap,last_day))
  }

  def extract(base_pt:String, source_name:String, feat_name:String, week_id:Int, interval:Int ): Unit ={
    println( "FeatOnGap")
    //获取候选集
    val candidate = DataLoad.load( base_pt + "/" + source_name ).filter{
      case ( uid, week, day, video_site, watch_count ) =>
        week < week_id && week >= (week_id - interval)
    }.map{
      case ( uid, week, day, video_site, watch_count ) => ( (uid , video_site) ,( ( week - 1) * 7 +  day, watch_count) )
    }

    val datas = candidate.groupBy( _._1 ).map { group =>
      //_._1 =>表示候选的标识， _._2 =>( (uid, vid), (day, wcount) )
      caculate(group, week_id)
    }.groupBy( _._1 ).map {
      group =>
        val uid = group._1
        val values = toMap[(Double, Int)](group._2)
        (uid,values)
    }

    val path = base_pt + "/" + feat_name
    print[(Double,Int)](
      path, datas, (data,did,vid,uid) =>{
        val (avg_gap,last_day) = data(vid)
        val day = ( week_id - 1 ) * 7 + did + 1
        val day_gap = day - last_day
        //如果差异小于1,则表示gap落在在当天
        if( ( day_gap - avg_gap*(day_gap/avg_gap).toInt ) < 1 )
          s"$did\t$vid\t$uid\t1"
        else s"$did\t$vid\t$uid\t0"
      },
      (did,vid,uid)=>{
        s"$did\t$vid\t$uid\t0"
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
