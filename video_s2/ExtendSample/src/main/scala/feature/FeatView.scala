package feature

import data.extract.DataLoad

/**
 * 往前1天的，3天的，7天的hit,cnt,sum特征
 */
object FeatView extends Feature{

  def slice(begin_day:Int, last_week: Array[(String, Int, Int, Int, Int)] ): Map[String, Array[(String, (Int, Int))]] ={
    last_week.filter{
      case (uid, week, day, vid, cnt ) =>
        day >= begin_day
    }.map{
      case (uid,week,day,vid,cnt) => (uid,(vid,cnt))
    }.groupBy( _._1 )
  }
  def extract(base_pt:String, source_name:String, feat_name:String, week_id:Int, interval:Int ): Unit ={
    println("FeatView")
    val data = DataLoad.load( base_pt + "/" + source_name ).filter{
      case ( uid, week, day, video_site, watch_count ) =>
        week < week_id && week >= (week_id - interval)
    }

    val last_week: Array[(String, Int, Int, Int, Int)] = data.filter{
      case ( uid, week, day, vid, cnt ) =>
        week == week_id - 1
    }

    val day_1 = slice(7,last_week)
    val day_3 = slice(5,last_week)
    val day_7 = slice(1,last_week)

    val datas = data.map{
      case ( uid, week, day, video_site, watch_count ) => (uid , video_site)
    }.toSet[(String,Int)].map { case (uid, vid) =>

      val fold_hit_1 = Array.fill[Int](10)(0)
      val fold_cnt_1 = Array.fill[Int](10)(0)
      val fold_sum_1 = Array.fill[Int](10)(0)
      val fold_hit_3 = Array.fill[Int](10)(0)
      val fold_cnt_3 = Array.fill[Int](10)(0)
      val fold_sum_3 = Array.fill[Int](10)(0)
      val fold_hit_7 = Array.fill[Int](10)(0)
      val fold_cnt_7 = Array.fill[Int](10)(0)
      val fold_sum_7 = Array.fill[Int](10)(0)

      if (day_1.contains(uid))
        day_1(uid).foreach {
          case (uid, (vid, cnt)) =>
            fold_hit_1(vid - 1) = 1
            fold_cnt_1(vid - 1) += 1
            fold_sum_1(vid - 1) += cnt
        }
      if (day_3.contains(uid))
        day_3(uid).foreach {
          case (uid, (vid, cnt)) =>
            fold_hit_3(vid - 1) = 1
            fold_cnt_3(vid - 1) += 1
            fold_sum_3(vid - 1) += cnt
        }

      if (day_7.contains(uid))
        day_7(uid).foreach {
          case (uid, (vid, cnt)) =>
            fold_hit_7(vid - 1) = 1
            fold_cnt_7(vid - 1) += 1
            fold_sum_7(vid - 1) += cnt
        }

      val lists = scala.collection.mutable.MutableList[Array[Int]]()

      lists += fold_hit_1
      lists += fold_hit_3
      lists += fold_hit_7
      lists += fold_cnt_1
      lists += fold_cnt_3
      lists += fold_cnt_7
      lists += fold_sum_1
      lists += fold_sum_3
      lists += fold_sum_7

      (uid, vid, lists)
    }.groupBy( _._1 ).map {
      group =>
        val uid = group._1
        val values = toMap[scala.collection.mutable.MutableList[Array[Int]]](group._2)
        (uid,values)
    }

    val path = base_pt + "/" + feat_name
    print[scala.collection.mutable.MutableList[Array[Int]]](
      path, datas, (data,did,vid,uid) =>{
        val lists = data(vid)
        val feat_line = new StringBuilder(s"$did\t$vid\t$uid")
        lists.foreach{
          case fold =>
            fold.foreach{
              x => feat_line.append(s"\t$x")
            }
        }
        feat_line.toString()
      },
      (did,vid,uid)=>{
        val feat_line = new StringBuilder(s"$did\t$vid\t$uid")
        for( i <- 0 until 90 )
          feat_line.append(s"\t0")
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
