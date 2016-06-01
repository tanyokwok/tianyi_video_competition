package feature

import java.io.PrintWriter

import data.extract.DataLoad
import feature.FeatStat._
import org.saddle.Vec

import scala.collection.immutable.Iterable

/**
  * 用户历史中所有记录的cnt,sum,mean,median,stdev,每周所有记录的cnt,sum,mean,median,stdev
  */
object FeatUser extends Feature {
  def extract(base_pt:String, source_name:String, feat_name:String, week_id:Int, interval:Int ): Unit ={
    println( "FeatUser")
    val feat_out = new PrintWriter(base_pt + "/" + feat_name )

    val begin_week = (week_id - interval)
    val end_week = week_id
    //获取候选集
    val data = DataLoad.load( base_pt + "/" + source_name ).filter{
      case ( uid, week, day, video_site, watch_count ) =>
        week < end_week && week >= begin_week
    }

    //用户历史中所有记录的cnt,sum,mean,median,stdev,每周所有记录的cnt,sum,mean,median,stdev
    val feats: Map[String, (Int, Int, Double, Double, Double, Int, Iterable[(Int, Int, Int, Double, Double, Double, Int)])]
    = data.groupBy( _._1 ).map{ x =>
      //_._1 =>表示候选的标识
      val uid = x._1

      //所有用户的历史点击情况
      val history = x._2.map{
        case (uid,week,day,vid,watch_count) =>
          watch_count
      }


      val vec_hist = Vec( history )

      //用户记录统计
      val hist_cnt = vec_hist.length
      val hist_sum = vec_hist.sum
      val hist_mean = vec_hist.mean
      val hist_median = vec_hist.median
      val hist_stdev = vec_hist.stdev
      //按照vid列聚合，然后获取keys
      val hist_vidcnt = x._2.groupBy(_._4).keys.size

      //按照周聚合 week => Seq( week, vid, watch_count )
      val week_history = x._2.map{
        case ( uid, week,day, vid, watch_count ) =>
          ( week, vid, watch_count )
      }.groupBy(_._1 )

      val week_feat = week_history.map{
        x =>

          //获取这周内用户的行为
          val actions = x._2.map{
            case ( week, vid, watch_count) =>
              watch_count
          }

          val vec = Vec(actions)
          val cnt = vec.length
          val sum = vec.sum
          val mean = vec.mean
          val median = vec.median
          val stdev = vec.stdev
          //获取用户查看网站的个数
          val vidcnt = x._2.map {
            case (week,vid,count ) =>
              vid
          }.toSet.size

          val week = x._1

          (week, cnt, sum, mean, median, stdev, vidcnt )
      }

      ( uid, ( hist_cnt, hist_sum, hist_mean, hist_median, hist_stdev, hist_vidcnt, week_feat ))
    }


    val path = base_pt + "/" + feat_name
    printUser[ (Int, Int, Double, Double, Double, Int,
      Iterable[(Int, Int, Int, Double, Double, Double, Int)])](
      path, feats, (feat,did,vid,uid) =>{
        val feat_line = new StringBuilder(s"$did\t$vid\t$uid\t${feat._1}\t${feat._2}\t${feat._3}\t" +
          s"${feat._4}\t${feat._5}\t${feat._6}")

        val week_feats: Map[Int, (Int, Int, Double, Double, Double, Int)] = feat._7.map{
          case (week, cnt, sum, mean, median, stdev, vidcnt ) =>
            ( week, (cnt, sum, mean, median, stdev, vidcnt))
        }.toMap

        for( i <- begin_week until end_week ){
          if( week_feats.contains( i )){
            val feat = week_feats(i)
            feat_line.append(s"\t${feat._1}\t${feat._2}\t${feat._3}\t${feat._4}\t${feat._5}\t${feat._6}")
          }else{
            feat_line.append(s"\t0\t0\t0\t0\t0\t0")
          }
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
