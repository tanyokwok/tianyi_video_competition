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
    }

    val target: Map[String, Map[Int, Map[Int, Int]]] = action_list.filter{
      case (uid,week,day,vid,cnt) =>
        week == week_id
    }.groupBy(_._1).map{
      grp =>
        //(day,count)
        val uid: String =   grp._1
        //按照vid group
        val targetMap: Map[Int, Map[Int, Int]] = grp._2.groupBy(_._4)
          .map{
          group =>
            val vid = group._1
            //按天group
            val day_map: Map[Int, Int] = group._2.map{
              case ( uid,week,day,vid,cnt) =>
                (day-1,cnt)
            }.toMap
            (vid,day_map)
        }
        ( uid, targetMap)
    }

    val datas = action_list.filter{
      case (uid,week,day,vid,cnt) =>
        week < week_id && week >= (week_id - interval)
    }.map{
      case ( uid, week, day, video_site, watch_count )
      => ( (uid , video_site) ,( ( week - 1) * 7 +  day, watch_count) )
    }.groupBy(_._1).map { x =>
      val (uid, vid) = x._1
      (uid, vid, () )
    }.groupBy(_._1).map {
      group =>
        val uid = group._1
        val values = toMap[Unit](group._2)
        (uid, values)
    }

    val path = base_pt + "/" + feat_name
    print[Unit](
      path, datas, (data, did, vid, uid) => {
        if( !target.contains(uid) || !target(uid).contains( vid )) s"$did\t$vid\t$uid\t0"
        else {
          val targetMap = target(uid)( vid )
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
