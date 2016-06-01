package feature

import data.extract.DataLoad

import scala.collection.immutable.IndexedSeq

/**
  * 计算前几周同一个工作日，<uid,vid,did>的访问次数
  */
class FeatWeekDay extends Feature{

  var feats:  Map[String, Map[Int, Map[Int, Array[(Int, (Int, Int))]]]] = null

  def featGen( data: Array[(String, Int, Int, Int, Int)], begin_week:Int, end_week:Int): Unit ={
    val candidate = data.map{
      case ( uid, week, day, video_site, watch_count ) => ( (uid , video_site) ,( week,day,watch_count) )
    }


    feats = candidate.groupBy( _._1 ).map { x =>
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
  }

  def getFeat(uid:String, vid:Int, did:Int, begin_week:Int, end_week:Int): String ={
    if( !feats.contains(uid) || !feats(uid).contains(vid) ) {

      val feat_line = new StringBuilder("")
      for( i <- begin_week until end_week){
          feat_line.append(s"\t0")
      }
      feat_line.toString()
    }
    else {
      val day_week_counts = feats(uid)(vid)
      //总过有interval周，每周一个hot
      val one_hot = Array.fill[Int](end_week - begin_week)(0)
      if (day_week_counts.contains(did + 1)) {
        //得到did这个工作日的数据
        val week_counts: Array[(Int, (Int, Int))] = day_week_counts(did + 1)
        week_counts.foreach {
          case (day, (week, count)) =>
            one_hot(week - begin_week) = count //这个工作日的访问次数
        }
      }

      val feat_line = new StringBuilder("")
      one_hot.foreach {
        x =>
          feat_line.append(s"\t$x")
      }
      feat_line.toString()
    }
  }
}
