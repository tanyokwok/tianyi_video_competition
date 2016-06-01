package feature

import java.io.PrintWriter

import data.extract.DataLoad
import feature.FeatStat._

/**
  * 计算前几周同一个工作日，<uid,vid>的访问次数
  */
object FeatWeekDay extends Feature{
  def extract(base_pt:String, source_name:String, feat_name:String, week_id:Int, interval:Int ): Unit ={
    println( "FeatWeekDay")

    val begin_week = (week_id - interval)
    val end_week = week_id
    //获取候选集
    val candidate = DataLoad.load( base_pt + "/" + source_name ).filter{
      case ( uid, week, day, video_site, watch_count ) =>
        week < end_week && week >= begin_week
    }.map{
      case ( uid, week, day, video_site, watch_count ) => ( (uid , video_site) ,( week,day,watch_count) )
    }

    val datas = candidate.groupBy( _._1 ).map { x =>
      //_._1 =>表示候选的标识， _._2 => Seq( (uid, vid), (week, wcount) )
      val (uid, vid) = x._1

      //按照工作日（1-7）汇总<day,(week,count)>
      val day_week_counts: Map[Int, Array[(Int, (Int, Int))]] = x._2.map {
        case ((uid, vid), (week, day, count)) =>
          (day, (week, count))
      }.groupBy(_._1)

      (uid, vid, day_week_counts)
    }.groupBy( _._1 ).map {
      group =>
        val uid = group._1
        val values = toMap[Map[Int, Array[(Int, (Int, Int))]]](group._2)
        (uid,values)
    }

    val path = base_pt + "/" + feat_name
    print[Map[Int, Array[(Int, (Int, Int))]]](
      path, datas, (data,did,vid,uid) =>{
        val day_week_counts = data(vid)
        //总过有interval周，每周一个hot
        val one_hot = Array.fill[Int]( interval )(0)
        if( day_week_counts.contains( did + 1 ) ) {
          //得到did这个工作日的数据
          val week_counts: Array[(Int, (Int, Int))] = day_week_counts(did + 1)
          week_counts.foreach{
            case ( day,(week, count) ) =>
              one_hot( week - begin_week ) = count//这个工作日的访问次数
          }
        }

        val feat_line = new StringBuilder( s"$did\t$vid\t$uid")
        one_hot.foreach{
          x =>
            feat_line.append(s"\t$x")
        }
        feat_line.toString()
      },
      (did,vid,uid)=>{
        val feat_line = new StringBuilder(s"$did\t$vid\t$uid")
        val one_hot = Array.fill[Int](interval)(0)
        one_hot.foreach {
          x =>
            feat_line.append(s"\t$x")
        }
        feat_line.toString()
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
