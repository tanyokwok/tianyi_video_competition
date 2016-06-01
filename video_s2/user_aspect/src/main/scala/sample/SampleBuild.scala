package sample

import java.io.PrintWriter

import data.extract.DataLoad
import feature.Feature
import feature.GenUserVideoFeats.Params
import scopt.OptionParser

import scala.io.Source

/**
  * Created by Administrator on 2016/2/26.
  */
object SampleBuild extends Feature{

  def extract(base_pt:String, source_name:String, cand_pt:String, feat_name:String, week_id:Int, interval:Int ): Unit ={
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
    val candidate = Source.fromFile(cand_pt).getLines().map{
      line =>
        val arr = line.split("\\s+")
        val vid = arr(0).toInt
        val uid = arr(1)
        (uid,vid)
    }

    val path = base_pt + "/" + feat_name
    val feat_out = new PrintWriter(path)
    candidate.foreach{
      case (uid,vid) =>
        Range(0,7).foreach {
          did =>
            val feat_line = new StringBuffer(s"${did}\t${vid}\t${uid}")
            if( !target.contains(uid) || !target(uid).contains( vid )) feat_line.append(s"\t0")
            else
            {
              val targetMap = target(uid)( vid )
              if (targetMap.contains(did))
                feat_line.append(s"\t${targetMap(did)}")
              else
                feat_line.append(s"\t0")
            }
            feat_out.println(feat_line.toString)
        }
    }
    feat_out.close()
  }


  def run(params: Params): Unit ={
    extract(params.base_pt,
      params.source_name,
      params.cand_pt,
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
