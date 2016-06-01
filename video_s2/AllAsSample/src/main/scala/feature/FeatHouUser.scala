package feature

import java.io.PrintWriter

import data.extract.DataLoad
import org.saddle.Vec

import scala.collection.immutable.Iterable

/**
  * 用户 1/3/7/14/28 天中，浏览10个网站的累积天数，user_day_cnt
  * 用户 1/3/7/14/28 天中，浏览10个网站的分别累积天数，user_day_cnt_dis
  * 用户在14天中，是否有访问记录
  * 用户对10个网站是否有浏览记录，site_visit_flag
  * 用户访问过10个网站中的几个，site_number
  */
object FeatHouUser extends Feature {

  //用户 1/3/7/14/28 天中，浏览10个网站的累积天数，user_day_cnt
  def calcUserDayCnt(data: Array[(String, Int, Int, Int, Int)])
  :List[Int]= {
    def calc(day_region:Int):Int = {
      data.filter {
        case (uid, week, day, vid, watch_count) =>
          day <= day_region
      }.length
    }
    List(calc(1),calc(3),calc(7),calc(14),calc(28))
  }

  def calcUserDayCntDis(data: Array[(String, Int, Int, Int, Int)])
  : List[Array[Int]] = {
    def calc(day_region:Int): Array[Int] ={
      //得到对应时间区间内的记录，并按照vid分组
      val group = data.filter{
        case (uid,week,day,vid,watch_count) =>
          day <= day_region
      }.groupBy(_._4)

      val one_hot = Array.fill[Int]( 10 )(0)
      Range(0,10).foreach{
        r =>
          val vid = r + 1
          if( group.contains( vid ) ) one_hot(r) = group.get(vid).size
      }

      one_hot
    }
    List(calc(1),calc(3),calc(7),calc(14),calc(28))
  }

  def calcUserHasRecord(data: Array[(String, Int, Int, Int, Int)]): Array[Int] = {
    val length = data.filter{
      case (uid,week,day,vid,watch_count) =>
        day <= 14
    }.length
    val hasRecord = if( length > 0 ) 1 else 0

    val group = data.groupBy( _._4 )
    val one_hot = Array.fill[Int]( 12 )(0)
    Range(1,11).foreach{
      vid =>
        if( group.contains( vid ) ) one_hot(vid) = if( group.get(vid).size > 0 ) 1 else 0
    }

    one_hot(11) = one_hot.sum//访问过几个网站
    one_hot(0) = hasRecord//是否有访问记录
    one_hot
  }

  def extract(base_pt:String, source_name:String, feat_name:String, week_id:Int, interval:Int ): Unit ={
    println( "FeatHouUser")
    val begin_week = (week_id - interval)
    val end_week = week_id
    //获取候选集
    val data = DataLoad.load( base_pt + "/" + source_name ).filter{
      case ( uid, week, day, video_site, watch_count ) =>
        week < end_week && week >= begin_week
    }.map{
      case( uid, week, day,video_site,watch_count ) =>
        val new_day = (end_week - week)*7 + 1 - day//(end_week - week - 1)*7 + 8 - day，表示比end_week往前多少天
        (uid,week,new_day,video_site,watch_count)
    }

    //用户历史中所有记录的cnt,sum,mean,median,stdev,每周所有记录的cnt,sum,mean,median,stdev
    val feats: Map[String, (List[Int], List[Array[Int]], Array[Int])] = data.groupBy( _._1 ).map{ x =>
      //_._1 =>表示候选的标识
      val uid = x._1
      (uid,(calcUserDayCnt( x._2),calcUserDayCntDis( x._2 ),calcUserHasRecord( x._2 )))
    }


    val path = base_pt + "/" + feat_name
    printUser[ (List[Int], List[Array[Int]], Array[Int])](
      path, feats, (feat,did,vid,uid) =>{
        val feat_line = new StringBuilder(s"$did\t$vid\t$uid")

        feat._1.foreach{
          f => feat_line.append(s"\t$f")
        }

        feat._2.foreach{
          arr => arr.foreach{
            f => feat_line.append(s"\t$f")
          }
        }

        feat._3.foreach{
          f => feat_line.append(s"\t$f")
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
