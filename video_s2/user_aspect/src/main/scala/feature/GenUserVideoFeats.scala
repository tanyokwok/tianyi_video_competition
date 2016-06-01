package feature

import java.io.PrintWriter

import data.extract.DataLoad
import scopt.OptionParser

import scala.io.Source
;

/**
 * Created by Administrator on 2016/3/12.
 */
object GenUserVideoFeats extends Feature{

  def extract(base_pt:String, source_name:String, cand_pt:String, feat_name:String, week_id:Int, interval:Int ): Unit ={
    println("GenFeats")
    val begin_week = week_id - interval
    val end_week = week_id
    val data: Array[(String, Int, Int, Int, Int)] = DataLoad.load( base_pt + "/" + source_name ).filter{
      case ( uid, week, day, video_site, watch_count ) =>
        week < week_id && week >= (week_id - interval)
    }

    val candidate = Source.fromFile(cand_pt).getLines().map{
      line =>
        val arr = line.split("\\s+")
        val vid = arr(0).toInt
        val uid = arr(1)
        (uid,vid)
    }

    val avgGap = new FeatAvgGap()
    val gap = new FeatGap()
    val ongap = new FeatOnGap()
    val stat = new FeatStat()
    val week = new FeatWeek()
    val weekday = new FeatWeekDay()

    avgGap.featGen(data,week_id)
    gap.featGen(data,week_id)
    ongap.featGen(data,week_id)
    stat.featGen(data)
    week.featGen(data,begin_week,end_week)
    weekday.featGen(data,begin_week,end_week)

    val path = base_pt + "/" + feat_name
    val feat_out = new PrintWriter(path)
    candidate.foreach{
      case (uid,vid) =>
        Range(0,7).foreach {
          did =>
            val feat_line = new StringBuffer(s"$did\t${vid}\t${uid}")
            feat_line.append(s"${avgGap.getFeat(uid, vid)}")
            feat_line.append(s"${gap.getFeat(uid, vid)}")
            feat_line.append(s"${ongap.getFeat(uid, vid,did,week_id)}")
            feat_line.append(s"\t$vid")
            feat_line.append(s"\t$did")
            feat_line.append(s"${stat.getFeat(uid, vid)}")
            feat_line.append(s"${week.getFeat(uid, vid,begin_week,end_week)}")
            feat_line.append(s"${weekday.getFeat(uid,vid,did,begin_week,end_week)}")
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
