package sample

import data.extract.DataLoad
import feature.Feature

/**
  * Created by Administrator on 2016/2/26.
  */
object SampleBuild extends Feature{

  def extract(base_pt:String, source_name:String, feat_name:String, week_id:Int, interval:Int ): Unit = {
    println("SampleBuild")

    //获得用户(uid,video_site)的行为列表
    val action_list = DataLoad.load(base_pt + "/" + source_name).filter {
      case (uid, week, day, video_site, watch_count) =>
        week <= week_id && week >= (week_id - interval)
    }.map {
      case (uid, week, day, video_site, watch_count)
      => ((uid, video_site), ((week - 1) * 7 + day, watch_count))
    }

    //获取有效的(uid, video_size)对
    val val_candidates = action_list.groupBy(_._1).filter {
      x =>
        val samples = x._2.map(y => y._2)

        //week_id之前的
        val day_threshold = (week_id - 1) * 7
        val history = samples.filter {
          case (day, count) =>
            day <= day_threshold//在week_id周之前的所有日期
        }
        history.size != 0
    }

    val datas = val_candidates.map { x =>

      val (uid, vid) = x._1
      //(day,count)
      val samples = x._2.map(y => y._2)

      val day_threshold = (week_id - 1) * 7

      val target = samples.filter {
        case (day, count) =>
          day > day_threshold
      }.map {
        case (day, count) =>
          (day - (day_threshold + 1), count)
      }

      val targetMap: Map[Int, Int] = target.toMap
      (uid, vid, targetMap)
    }.groupBy(_._1).map {
      group =>
        val uid = group._1
        val values = toMap[Map[Int, Int]](group._2)
        (uid, values)
    }

    val path = base_pt + "/" + feat_name
    print[Map[Int, Int]](
      path, datas, (data, did, vid, uid) => {
        if( !data.contains( vid )  ) s"$did\t$vid\t$uid\t0"
        else {
          val targetMap = data(vid)
          if (targetMap.contains(did))
            s"$did\t$vid\t$uid\t${targetMap(did)}"
          else
            s"$did\t$vid\t$uid\t0"
        }
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
