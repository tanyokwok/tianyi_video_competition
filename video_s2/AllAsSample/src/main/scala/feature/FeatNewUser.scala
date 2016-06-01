package feature

import java.io.PrintWriter

import data.extract.DataLoad
import org.saddle.Vec

import scala.collection.immutable.Iterable

/**
  *  用户在每个工作日的特征，是否看过视频，看视频的统计(cnt,sum),看视频的概率(cntprob,sumprob)
  */
object FeatNewUser extends Feature {
  def extract(base_pt:String, source_name:String, feat_name:String, week_id:Int, interval:Int ): Unit ={
    println( "FeatNewUser")
    val feat_out = new PrintWriter(base_pt + "/" + feat_name )

    val begin_week = (week_id - interval)
    val end_week = week_id
    //获取候选集
    val data = DataLoad.load( base_pt + "/" + source_name ).filter{
      case ( uid, week, day, video_site, watch_count ) =>
        week < end_week && week >= begin_week
    }

    val feats
    = data.groupBy( _._1 ).map{ x =>
      //_._1 =>表示候选的标识
      val uid = x._1


      val hit_ohe = Array.fill[Int](7)(0)
      val cnt_ohe = Array.fill[Int](7)(0)
      val sum_ohe = Array.fill[Int](7)(0)
      //按照工作日group
      x._2.groupBy( _._3 ).map{
        group =>
          val day = group._1
          val entry: Array[(String, Int, Int, Int, Int)] = group._2
          hit_ohe( day - 1) = 1
          cnt_ohe( day - 1) = entry.size
          sum_ohe( day - 1) = entry.map{
            case (uid,week,day,vid,cnt) => cnt
          }.sum
      }
      val cnt_sum = cnt_ohe.sum
      val total_sum = sum_ohe.sum
      val cnt_prob = cnt_ohe.map{  x => x.toDouble/cnt_sum }

      val sum_prob = sum_ohe.map{ x => x.toDouble/total_sum}

      ( uid, (hit_ohe,cnt_ohe,sum_ohe,cnt_prob,sum_prob))
    }

    val path = base_pt + "/" + feat_name
    printUser[ (Array[Int],Array[Int],Array[Int],Array[Double], Array[Double]) ](
      path, feats, (feat,did,vid,uid) =>{
        val feat_line = new StringBuilder(s"$did\t$vid\t$uid\t${feat._1(did)}" +
          s"\t${feat._2(did)}\t${feat._3(did)}\t${feat._4(did)}\t${feat._5(did)}")
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
