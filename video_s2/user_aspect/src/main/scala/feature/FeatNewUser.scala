package feature

import java.io.PrintWriter

import data.extract.DataLoad
import org.saddle.Vec

import scala.collection.immutable.Iterable
import scala.collection.mutable.StringBuilder

/**
  *  用户在每个工作日的特征，是否看过视频，看视频的统计(cnt,sum),看视频的概率(cntprob,sumprob)
  */
class FeatNewUser extends Feature {
  var feats: Map[String, (Array[Int], Array[Int], Array[Int], Array[Double], Array[Double])] = null
  def featGen( data: Array[(String, Int, Int, Int, Int)]): Unit ={

    feats = data.groupBy( _._1 ).map{ x =>
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
  }

  def getFeat(uid:String):String ={
    if( !feats.contains(uid)) {
      val feat_line = new StringBuilder("")

      Range(0,7).foreach{
        did =>
          feat_line.append(s"\t0\t0\t0\t0\t0")
      }
      feat_line.toString()
    } else{
        val feat_line = new StringBuilder("")
        val feat = feats(uid)
        Range(0,7).foreach{
          did =>
            feat_line.append(s"\t${feat._1(did)}" +
              s"\t${feat._2(did)}\t${feat._3(did)}\t${feat._4(did)}\t${feat._5(did)}")
        }
        feat_line.toString()

      }
    }
}
