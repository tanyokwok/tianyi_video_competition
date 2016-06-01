package feature

import org.saddle.Vec

import scala.collection.immutable.IndexedSeq

/**
  * 计算<uid,vid>每周的特征，包括是否访问,cnt,sum,median,mean,stdev
  */
class FeatWeek extends Feature{
  var feats: Map[String, Map[Int, IndexedSeq[(Int, Int, Int, Double, Double, Double)]]] = null

  def featGen( data: Array[(String, Int, Int, Int, Int)], begin_week:Int, end_week:Int): Unit ={
    val candidate =  data.map{
      case ( uid, week, day, video_site, watch_count )
      =>  ( (uid , video_site) ,( week, watch_count) )
    }

    feats = candidate.groupBy( _._1 ).map { x =>
      //_._1 =>表示候选的标识， _._2 => Seq( (uid, vid), (week, wcount) )
      val (uid, vid) = x._1

      //转换为每周一个列表
      val week_counts = x._2.map {
        case ((uid, vid), (week, count)) =>
          (week, count)
      }.groupBy(_._1)

      //每周的<uid,vid,did> cnt,sum,median,mean,stdev
      val feats: Map[Int, (Int, Int, Double, Double, Double)] = week_counts.map {
        x =>
          val week = x._1

          val counts: Array[Int] = x._2.map {
            case (week, cnt) =>
              cnt
          }

          //
          val vec = Vec(counts)
          val cnt = vec.length
          val sum = vec.sum
          val median = vec.median
          val mean = vec.mean
          val stdev = vec.stdev
          (week, (cnt, sum, median, mean, stdev))
      }

      val week_feats: IndexedSeq[(Int, Int, Int, Double, Double, Double)] = Range(begin_week, end_week).map {
        wk =>
          if (!feats.contains(wk)) (0, 0, 0, 0.0, 0.0, 0.0)
          else {
            val x = feats(wk)
            (1, x._1, x._2, x._3, x._4, x._5)
          }
      }

      (uid, vid, week_feats)
    }.groupBy( _._1 ).map {
      group =>
        val uid = group._1
        val values = toMap[IndexedSeq[(Int, Int, Int, Double, Double, Double)]](group._2)
        (uid,values)
    }
  }


  def getFeat(uid:String,vid:Int, begin_week:Int, end_week:Int): String ={

    if( !feats.contains(uid)|| !feats(uid).contains(vid) ) {
      val feat_line = new StringBuilder("")
      for( i <- begin_week until end_week ){
        feat_line.append(s"\t0\t0\t0\t0\t0\t0")
      }
      feat_line.toString()
    }
    else{
      val week_feats = feats(uid)(vid)
      val feat_line = new StringBuilder("")
      week_feats.foreach{
        case (f1,f2,f3,f4,f5,f6) =>
          feat_line.append(s"\t$f1\t$f2\t$f3\t$f4\t$f5\t$f6")
      }
      feat_line.toString()
    }
  }
}
