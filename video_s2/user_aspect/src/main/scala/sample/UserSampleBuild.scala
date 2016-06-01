package sample

import java.io.PrintWriter

import data.extract.DataLoad
import feature.Feature
import scopt.OptionParser

/**
  * Created by Administrator on 2016/2/26.
  */
object UserSampleBuild extends Feature{

  def extract(base_pt:String, source_name:String, feat_name:String, week_id:Int, interval:Int ): Unit ={
    println("ClassSampleBuild")

    //获得用户(uid,video_site)的行为列表
    val action_list = DataLoad.load(base_pt + "/" + source_name).filter {
      case (uid, week, day, video_site, watch_count) =>
        week <= week_id && week >= (week_id - interval)
    }

    val target: Map[String, Int] = action_list.filter{
      case (uid,week,day,vid,cnt) =>
        week == week_id
    }.groupBy(_._1).map{
      grp =>
        //(day,count)
        val uid: String =   grp._1
        //按照vid group
        val target = grp._2.groupBy(_._4)
          .map{
          group =>
            val vid = group._1
            //按天group
            val sum = group._2.map{
              case ( uid,week,day,vid,cnt) =>
                cnt
            }.sum
              sum
        }.sum
        ( uid, if( target > 0 ) 1 else 0 )
    }

    val candidate = action_list.filter{
      case (uid, week, day, video_site, watch_count) => uid
        week < week_id && week >= (week_id - interval)
    }.map {
      case (uid, week, day, video_site, watch_count) => uid
    }.toSet.toArray.sorted

    val path = base_pt + "/" + feat_name
    val feat_out = new PrintWriter(path)
    candidate.foreach{
      case uid =>
            val feat_line = new StringBuffer(s"${uid}")
            if( !target.contains(uid) ) feat_line.append(s"\t0")
            else
            {
              val t = target(uid)
              feat_line.append(s"\t$t")
            }
            feat_out.println(feat_line.toString)
    }
    feat_out.close()
  }


  def run(params: Params): Unit ={
    extract(params.base_pt,
      params.source_name,
      params.feat_name,
      params.week_id,
      params.interval)
  }

  def main(args: Array[String]): Unit ={

    val default_params = Params()
    parser.parse(args,default_params) match {
      case Some(params) => run(params)
      case None => System.exit(1)
    }

  }
  val parser = new OptionParser[Params]("Feature"){
    opt[String]("base_pt")
      .text("输入输出文件工作目录")
      .action{ (x,c) => c.copy(base_pt = x) }
    opt[String]("source_name")
      .text("输入文件名")
      .action{ (x,c) => c.copy( source_name = x) }
    opt[String]("cand_pt")
      .text("候选集文件名")
      .action{ (x,c) => c.copy( cand_pt = x) }
    opt[String]("feat_name")
      .text("特征文件名")
      .action{ (x,c) => c.copy( feat_name = x) }
    opt[Int]("week_id")
      .text("作为样本的周")
      .action{ (x,c) => c.copy( week_id = x) }
    opt[Int]("interval")
      .text("时间窗口（周）")
      .action{ (x,c) => c.copy( interval = x) }
  }

  case class Params(base_pt:String = "",
                    cand_pt:String = "",
                    source_name:String = "",
                    feat_name:String = "",
                    week_id:Int = 9,
                    interval:Int = 9)
}
